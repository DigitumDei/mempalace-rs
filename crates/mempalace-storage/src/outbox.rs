//! Durable replication outbox storage (issue #127 slice 1).
//!
//! Local-first asynchronous replication needs a mutation persisted before any network write, so a
//! crash cannot leave the local palace and a remote replica disagreeing about what was meant to be
//! sent. This module supplies that substrate as a SQLite-backed outbox with a recoverable intent
//! lifecycle plus an atomic claim lease, bounded retries, lease-expiry recovery, and terminal
//! transitions, all backed by the palace's operational SQLite database.
//!
//! # Crash-safe intent lifecycle
//!
//! For LanceDB + SQLite crash safety, the *intent* must be durable before the local mutation, but
//! must not be deliverable until the local commit is confirmed. Every [`OutboxStore::enqueue`]
//! therefore lands in the non-deliverable [`OutboxState::Staged`] state, where no claim path can
//! reach it. After the local mutation commits, [`OutboxStore::activate`] marks it
//! [`OutboxState::Pending`] and it becomes eligible for delivery. A staged intent whose local
//! mutation never committed is abandoned with [`OutboxStore::cancel`] to the terminal
//! [`OutboxState::Cancelled`] state. Startup reconciliation — the caller that re-checks each
//! staged intent against the logical entity (activate a staged add when the drawer exists,
//! activate a staged delete when it is absent/tombstoned, cancel an uncommitted mutation) — is
//! supported by [`OutboxStore::list_staged`] plus the two CAS transitions.
//!
//! # Delivery order
//!
//! `ordering_key` identifies a partition/group (e.g. one logical drawer or entity), not a global
//! priority. Claiming may pick any *due head* across groups, and a candidate `c` is eligible only
//! when no earlier operation currently active for delivery — `staged`, `pending`, `leased`, or
//! `retryable` —
//! shares `c`'s `(destination_remote, ordering_key)` with `p.sequence < c.sequence`. An in-flight,
//! retry-not-yet-due, or staged predecessor therefore blocks only its own group and never starves
//! unrelated groups. Terminal operations (replicated, failed, cancelled) stop blocking. Staged
//! operations are not claimable, but remain an ordering barrier until startup reconciliation
//! activates or cancels them. Ordering within a group follows the global insertion `sequence`.
//!
//! # State machine
//!
//! ```text
//! staged --activate--> pending --claim--> leased --acknowledge--> replicated
//!    \--cancel--> cancelled
//!
//! leased --schedule_retry (retryable, indefinitely)--> retryable --claim/--> leased
//! leased --fail (authoritative permanent error)-------------> failed
//! ```
//!
//! A leased operation whose lease expires is recovered by [`OutboxStore::claim_by_id`] or
//! [`OutboxStore::reclaim_expired_lease`]; a retryable operation becomes claimable again once its
//! `retry_after` passes. `schedule_retry` always keeps a transport/unknown failure retryable —
//! `attempt_count` stays observable but never exhausts into a terminal state, so arbitrarily long
//! remote outages simply keep retrying within the store. Terminal [`OutboxState::Failed`] is
//! entered only through [`OutboxStore::fail`] for an authoritative permanent error. Terminal
//! failures stay observable through [`OutboxStore::list_failed`] and the failure counts on
//! [`OutboxStore::backlog`].

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::types::RevisionedWrite;
use crate::{Result, StorageError};

/// Federation mutation payloads carry entire drawer records. The federation batch path allows a
/// 16 MiB body (the federation server's ingest limit), so a single outbox payload must be able to
/// hold a full mutation at that scale. Deliberately far above the 1 MiB coordination payload cap:
/// an outbox row is a machine mutation awaiting wire delivery, not a human-readable task note.
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 1024;
const MAX_IDENTIFIER_BYTES: usize = 256;

/// Leading fragments of the message each conflict-shaped [`StorageError::Invariant`] this module
/// can raise is built from. Mirrors [`crate::coordination`]'s pinned-constant discipline so a
/// future federation layer can classify outbox conflicts by exact text without re-deriving it.
/// `error_messages_start_with_their_pinned_constants` drives every path and asserts the produced
/// message actually starts with its constant.
/// Produced by [`OutboxStore::claim_by_id`] when another worker holds a live lease.
pub const OUTBOX_LEASE_HELD_BY_ANOTHER_WORKER: &str =
    "outbox operation lease is held by another worker";
/// Produced by [`OutboxStore::acknowledge`], [`OutboxStore::schedule_retry`] and
/// [`OutboxStore::fail`] when the caller is not the operation's current lease owner.
pub const OUTBOX_ONLY_LEASE_OWNER: &str = "only the lease owner may perform this action";
/// Produced by [`OutboxStore::acknowledge`], [`OutboxStore::schedule_retry`] and
/// [`OutboxStore::fail`] when the operation is not currently leased/in-flight.
pub const OUTBOX_OPERATION_NOT_IN_FLIGHT: &str = "outbox operation is not in flight";
/// Produced by methods that mutate a live lease when the operation's lease has already expired.
pub const OUTBOX_LEASE_HAS_EXPIRED: &str = "outbox operation lease has expired";
/// Produced by [`OutboxStore::claim_by_id`] when the operation is already in a terminal state.
pub const OUTBOX_OPERATION_TERMINAL: &str = "terminal outbox operation cannot be claimed";
/// Produced by [`OutboxStore::claim_by_id`] when a retryable operation's `retry_after` has not yet
/// passed. [`OutboxStore::claim_next`] skips such operations silently instead.
pub const OUTBOX_RETRY_NOT_DUE: &str = "outbox operation retry is not due yet";
/// Produced by [`OutboxStore::activate`] when the operation is not currently staged.
pub const OUTBOX_ONLY_STAGED_MAY_ACTIVATE: &str = "only a staged outbox operation may be activated";
/// Produced by [`OutboxStore::cancel`] when the operation is not staged.
pub const OUTBOX_CANCELLABLE_STATE_REQUIRED: &str =
    "only a staged outbox operation may be cancelled";
/// Produced by [`OutboxStore::claim_by_id`] when the operation is still staged: the local
/// mutation it records has not been confirmed committed, so it must not be delivered.
/// [`OutboxStore::claim_next`] skips staged operations silently instead.
pub const OUTBOX_OPERATION_NOT_ACTIVATED: &str =
    "staged outbox operation must be activated before it can be claimed";
/// Produced by [`OutboxStore::claim_by_id`] when an earlier nonterminal operation with the same
/// `(destination_remote, ordering_key)` and a smaller `sequence` is still live. That operation
/// blocks its own group only; unrelated groups are unaffected.
pub const OUTBOX_PREDECESSOR_IN_FLIGHT: &str = "outbox operation has an earlier nonterminal predecessor with the same destination remote and ordering key";
/// Produced by every lease-taking method when `now + ttl` would fall outside the range
/// `OffsetDateTime` can represent. `time::OffsetDateTime`'s `Add<Duration>` panics in that case,
/// so the call sites use `checked_add` and turn `None` into this error instead of aborting.
pub const OUTBOX_LEASE_DURATION_OUT_OF_RANGE: &str = "outbox lease duration is out of range";

/// Lifecycle state of a replication outbox operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxState {
    /// Intent persisted, but the local mutation it records has not been confirmed committed.
    /// Not deliverable: no claim path returns a staged operation. It remains an ordering barrier
    /// for later operations in the same group until it is activated (commit confirmed) or
    /// cancelled (uncommitted mutation). Startup reconciliation decides every staged intent before
    /// delivery starts.
    Staged,
    /// Local commit confirmed; awaiting a claim.
    Pending,
    /// Claimed by a worker and in flight under a lease.
    Leased,
    /// Delivery attempt failed; eligible for re-claim once `retry_after` passes.
    Retryable,
    /// Acknowledged as delivered; terminal.
    Replicated,
    /// Terminal failure after attempting, or an explicit `fail`.
    Failed,
    /// Abandoned before delivery (e.g. an uncommitted mutation cancelled during startup
    /// reconciliation); terminal.
    Cancelled,
}

impl OutboxState {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "staged" => Ok(Self::Staged),
            "pending" => Ok(Self::Pending),
            "leased" => Ok(Self::Leased),
            "retryable" => Ok(Self::Retryable),
            "replicated" => Ok(Self::Replicated),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(StorageError::Invariant(format!("unknown outbox state `{value}`"))),
        }
    }
    fn terminal(self) -> bool {
        matches!(self, Self::Replicated | Self::Failed | Self::Cancelled)
    }
}

/// Input for an idempotent outbox enqueue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewOutboxOperation {
    /// Actor enqueuing the operation; scopes the idempotency key.
    pub created_by: String,
    /// Idempotency key, unique per `created_by`. Replaying an enqueue returns the existing
    /// operation untouched.
    pub idempotency_key: String,
    /// What kind of mutation this operation represents (e.g. `drawer_added`, `fact_changed`).
    pub mutation_kind: String,
    /// Logical entity id the mutation applies to.
    pub entity_id: String,
    /// Destination remote this operation is bound for. Replication to different remotes is
    /// independent, and delivery order is enforced per `(destination_remote, ordering_key)`.
    pub destination_remote: String,
    /// Partition/group identity: everything sharing a `destination_remote` and `ordering_key`
    /// must be delivered in `sequence` order, and a live head blocks only that group. It is *not*
    /// a global priority — unrelated keys stay independent.
    pub ordering_key: String,
    /// Serialized mutation payload.
    pub payload: Value,
    /// Advisory upper bound on delivery attempts, retained for observability. Retryable
    /// transport/unknown failures stay retryable indefinitely regardless of this value; terminal
    /// [`OutboxState::Failed`] is entered only via an explicit [`OutboxStore::fail`] for an
    /// authoritative permanent error.
    pub max_attempts: i64,
}

/// Authoritative replication outbox operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboxOperation {
    /// Stable opaque operation identifier.
    pub operation_id: String,
    /// Global, insertion-ordered handle; monotonically increases across every entity and is the
    /// ordering key within a `(destination_remote, ordering_key)` group.
    pub sequence: i64,
    /// Actor that enqueued the operation; scopes the idempotency key.
    pub created_by: String,
    /// Idempotency key, unique per `created_by`.
    pub idempotency_key: String,
    /// What kind of mutation this operation represents.
    pub mutation_kind: String,
    /// Logical entity the mutation applies to.
    pub entity_id: String,
    /// Remote the operation is bound for.
    pub destination_remote: String,
    /// Partition/group identity (see [`NewOutboxOperation::ordering_key`]).
    pub ordering_key: String,
    /// Monotonic per-`(entity_id, destination_remote)` sequence, assigned at enqueue time.
    pub entity_sequence: i64,
    /// Current lifecycle state.
    pub state: OutboxState,
    /// Optimistic-concurrency revision; every transition carries an expected one.
    pub revision: i64,
    /// Worker currently holding the lease, when in flight.
    pub lease_owner: Option<String>,
    /// Expiry of the current lease; absent when not in flight.
    #[serde(with = "time::serde::rfc3339::option")]
    pub lease_expires_at: Option<OffsetDateTime>,
    /// Number of delivery attempts recorded so far.
    pub attempt_count: i64,
    /// Advisory upper bound on delivery attempts, retained for observability (never enforced as a
    /// terminal threshold).
    pub max_attempts: i64,
    /// Earliest time a retryable operation may be claimed again. `None` for non-retryable states.
    #[serde(with = "time::serde::rfc3339::option")]
    pub retry_after: Option<OffsetDateTime>,
    /// Most recent delivery error, retained across retries and cleared on acknowledgement.
    pub last_error: Option<String>,
    /// Serialized mutation payload.
    pub payload: Value,
    /// When the operation was enqueued.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// When the operation was last transitioned.
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// Backlog/status summary of the outbox, scoped to one remote or across all remotes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboxBacklog {
    /// The remote the summary is scoped to, or `None` for the whole outbox.
    pub destination_remote: Option<String>,
    /// Number of operations awaiting their first claim.
    pub pending_count: i64,
    /// Number of operations whose local commit is not yet confirmed (non-deliverable).
    pub staged_count: i64,
    /// Created-at of the oldest pending operation.
    #[serde(with = "time::serde::rfc3339::option")]
    pub oldest_pending_at: Option<OffsetDateTime>,
    /// Number of operations currently leased/in flight.
    pub leased_count: i64,
    /// Number of operations awaiting a scheduled retry.
    pub retryable_count: i64,
    /// Attempt count of the oldest retryable operation (0 when none are retryable).
    pub retry_attempt_count: i64,
    /// Last recorded error of the oldest retryable operation.
    pub retry_last_error: Option<String>,
    /// Number of operations that reached terminal failure.
    pub failed_count: i64,
    /// Number of abandoned (cancelled) operations.
    pub cancelled_count: i64,
    /// Total operations in every state.
    pub total_count: i64,
}

/// SQLite-backed durable replication outbox repository.
#[derive(Debug, Clone)]
pub struct OutboxStore {
    path: PathBuf,
}

impl OutboxStore {
    /// Open the outbox in the palace's operational SQLite database.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self { path: path.as_ref().to_path_buf() }
    }

    /// Install the outbox table and indexes. Idempotent and safe to call on every startup:
    /// `CREATE TABLE/INDEX IF NOT EXISTS` are no-ops against existing objects.
    pub fn ensure_schema(&self) -> Result<()> {
        let conn = self.connection()?;
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS replication_outbox (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 operation_id TEXT UNIQUE NOT NULL,
 created_by TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 mutation_kind TEXT NOT NULL, entity_id TEXT NOT NULL, destination_remote TEXT NOT NULL,
 ordering_key TEXT NOT NULL, entity_sequence INTEGER NOT NULL,
 state TEXT NOT NULL, revision INTEGER NOT NULL,
 lease_owner TEXT, lease_expires_at TEXT,
 attempt_count INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
 retry_after TEXT, last_error TEXT,
 payload_json TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(created_by, idempotency_key),
 UNIQUE(entity_id, destination_remote, entity_sequence));
CREATE INDEX IF NOT EXISTS idx_replication_outbox_claim
 ON replication_outbox(destination_remote, state, sequence);
"#,
        )?;
        Ok(())
    }

    /// Enqueue an operation *in the staged state*, or return the prior committed operation for an
    /// idempotency replay.
    ///
    /// The returned operation is **[`OutboxState::Staged`]** — it is durable but not deliverable
    /// until the local mutation it records commits and the caller confirms it with
    /// [`Self::activate`]. If the local mutation never commits, the intent is abandoned with
    /// [`Self::cancel`]. See the module docs and `docs/` federation design for the crash-safety
    /// rationale.
    ///
    /// The per-`(entity_id, destination_remote)` sequence is assigned inside the same transaction
    /// as the insert, so a replay never advances it and concurrent enqueuers per entity cannot
    /// race to the same value.
    pub fn enqueue(&self, input: &NewOutboxOperation) -> Result<OutboxOperation> {
        validate_key(&input.idempotency_key)?;
        bounded_identifier(&input.created_by, "created_by")?;
        bounded_identifier(&input.mutation_kind, "mutation_kind")?;
        nonempty_identifier(&input.entity_id, "entity_id")?;
        bounded_identifier(&input.destination_remote, "destination_remote")?;
        nonempty_identifier(&input.ordering_key, "ordering_key")?;
        bounded_json(&input.payload)?;
        if input.max_attempts < 1 {
            return Err(StorageError::Invariant("max_attempts must be at least 1".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(op) = find_operation_by_key(&tx, &input.created_by, &input.idempotency_key)? {
            if op.mutation_kind != input.mutation_kind
                || op.entity_id != input.entity_id
                || op.destination_remote != input.destination_remote
                || op.ordering_key != input.ordering_key
                || op.max_attempts != input.max_attempts
                || op.payload != input.payload
            {
                return Err(StorageError::Invariant(format!(
                    "outbox idempotency key `{}` was reused with a different mutation",
                    input.idempotency_key
                )));
            }
            if op.state == OutboxState::Cancelled {
                let now = OffsetDateTime::now_utc();
                tx.execute(
                    "UPDATE replication_outbox SET state='staged',revision=revision+1,updated_at=?2 \
                     WHERE operation_id=?1 AND state='cancelled'",
                    params![op.operation_id, format_time(now)?],
                )?;
                let restored = get_operation_tx(&tx, &op.operation_id)?.ok_or_else(|| {
                    StorageError::Invariant("restored operation disappeared".into())
                })?;
                tx.commit()?;
                return Ok(restored);
            }
            tx.commit()?;
            return Ok(op);
        }
        let now = OffsetDateTime::now_utc();
        let next_sequence: i64 = tx.query_row(
            "SELECT COALESCE(MAX(entity_sequence),0)+1 FROM replication_outbox \
             WHERE entity_id=?1 AND destination_remote=?2",
            params![input.entity_id, input.destination_remote],
            |row| row.get(0),
        )?;
        let id = format!("outbox_{}", Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO replication_outbox(operation_id,created_by,idempotency_key,mutation_kind,\
             entity_id,destination_remote,ordering_key,entity_sequence,state,revision,lease_owner,\
             lease_expires_at,attempt_count,max_attempts,retry_after,last_error,payload_json,\
             created_at,updated_at)\
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'staged',0,NULL,NULL,0,?9,NULL,NULL,?10,?11,?11)",
            params![
                id,
                input.created_by,
                input.idempotency_key,
                input.mutation_kind,
                input.entity_id,
                input.destination_remote,
                input.ordering_key,
                next_sequence,
                input.max_attempts,
                serde_json::to_string(&input.payload)?,
                format_time(now)?,
            ],
        )?;
        let op = get_operation_tx(&tx, &id)?
            .ok_or_else(|| StorageError::Invariant("enqueued operation disappeared".into()))?;
        tx.commit()?;
        Ok(op)
    }

    /// Retrieve an operation by exact ID. `None` is an explicit authoritative miss.
    pub fn get_operation(&self, operation_id: &str) -> Result<Option<OutboxOperation>> {
        let conn = self.connection()?;
        get_operation_conn(&conn, operation_id)
    }

    /// Retrieve an operation by its caller idempotency key, scoped to one `created_by`.
    /// `None` is an explicit authoritative miss.
    ///
    /// This is the replay-recovery read for callers that hold only their stable
    /// operation id (which is stored verbatim as the idempotency key) rather than the
    /// generated `outbox_*` identifier: e.g. a keyed `write:both` delete retried after
    /// the local drawer is gone, which must recover the original queued/terminal
    /// operation instead of re-resolving a route from local metadata.
    pub fn find_by_key(
        &self,
        created_by: &str,
        idempotency_key: &str,
    ) -> Result<Option<OutboxOperation>> {
        let conn = self.connection()?;
        conn.prepare(&format!("{OPERATION_COLUMNS} WHERE created_by=?1 AND idempotency_key=?2"))?
            .query_row([created_by, idempotency_key], operation_row)
            .optional()
            .map_err(Into::into)
    }

    /// List staged operations (oldest first) — the startup-reconciliation feed.
    ///
    /// Reconciliation decides each staged intent against the local logical entity, then calls
    /// [`Self::activate`] (commit confirmed) or [`Self::cancel`] (uncommitted mutation) per
    /// operation with its current revision.
    pub fn list_staged(&self, limit: usize) -> Result<Vec<OutboxOperation>> {
        let conn = self.connection()?;
        let limit = limit.clamp(1, 10_000) as i64;
        let mut statement = conn.prepare(&format!(
            "{OPERATION_COLUMNS} WHERE state='staged' ORDER BY sequence LIMIT ?1"
        ))?;
        statement
            .query_map([limit], operation_row)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// List terminally failed operations (most recent failure first) for observability.
    pub fn list_failed(&self, limit: usize) -> Result<Vec<OutboxOperation>> {
        let conn = self.connection()?;
        let limit = limit.clamp(1, 10_000) as i64;
        let mut statement = conn.prepare(&format!(
            "{OPERATION_COLUMNS} WHERE state='failed' ORDER BY updated_at DESC, sequence DESC LIMIT ?1"
        ))?;
        statement
            .query_map([limit], operation_row)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Confirm that the local mutation behind a staged operation committed, moving it to
    /// [`OutboxState::Pending`] where it becomes deliverable.
    ///
    /// Compare-and-swap on `expected_revision`. A stale revision returns
    /// `Ok(`[`RevisionedWrite::Conflict`]`)`; activating a non-staged operation is an `Err` built
    /// from [`OUTBOX_ONLY_STAGED_MAY_ACTIVATE`].
    pub fn activate(
        &self,
        operation_id: &str,
        expected_revision: i64,
    ) -> Result<RevisionedWrite<OutboxOperation>> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(op) = get_operation_tx(&tx, operation_id)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        if op.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(op.revision) });
        }
        if op.state != OutboxState::Staged {
            return Err(StorageError::Invariant(OUTBOX_ONLY_STAGED_MAY_ACTIVATE.into()));
        }
        let now = OffsetDateTime::now_utc();
        let changed = tx.execute(
            "UPDATE replication_outbox SET state='pending',revision=revision+1,updated_at=?2 \
             WHERE operation_id=?1 AND revision=?3",
            params![operation_id, format_time(now)?, expected_revision],
        )?;
        if changed != 1 {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        }
        let result = get_operation_tx(&tx, operation_id)?
            .ok_or_else(|| StorageError::Invariant("activated operation disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Abandon a staged operation before its local mutation commits, moving it to the terminal
    /// [`OutboxState::Cancelled`] state.
    ///
    /// This is the reconciliation path for an intent whose local mutation never committed. Once an
    /// operation has been activated, the local commit is confirmed and the durable replication
    /// obligation must not be silently abandoned — a pending or in-flight operation resolves only
    /// through acknowledgement, retry, or [`Self::fail`]. Compare-and-swap on `expected_revision`.
    pub fn cancel(
        &self,
        operation_id: &str,
        expected_revision: i64,
    ) -> Result<RevisionedWrite<OutboxOperation>> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(op) = get_operation_tx(&tx, operation_id)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        if op.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(op.revision) });
        }
        if op.state != OutboxState::Staged {
            return Err(StorageError::Invariant(OUTBOX_CANCELLABLE_STATE_REQUIRED.into()));
        }
        let now = OffsetDateTime::now_utc();
        let changed = tx.execute(
            "UPDATE replication_outbox SET state='cancelled',revision=revision+1,updated_at=?2 \
             WHERE operation_id=?1 AND revision=?3 AND state='staged'",
            params![operation_id, format_time(now)?, expected_revision],
        )?;
        if changed != 1 {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        }
        let result = get_operation_tx(&tx, operation_id)?
            .ok_or_else(|| StorageError::Invariant("cancelled operation disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Atomically claim the next deliverable operation for `destination_remote`.
    ///
    /// Eligible candidates are **due heads**: a `pending` operation, or a `retryable` one whose
    /// `retry_after` has passed, with no earlier operation currently active for delivery (`p.state
    /// IN staged/pending/leased/retryable`) sharing the same `(destination_remote, ordering_key)`
    /// and `p.sequence < c.sequence`. An active predecessor blocks only its own group; staged
    /// operations are not claimable themselves, but remain a barrier until startup reconciliation
    /// activates or cancels them. Among due heads the lowest
    /// `sequence` wins, and **every** group head is considered: the scan is never truncated, so an
    /// eligible due head can never be hidden behind blocked or not-yet-due groups.
    ///
    /// Contention is excluded by running the read and the lease write inside one
    /// `BEGIN IMMEDIATE` transaction, so concurrent workers on the same file observe the winner's
    /// `leased` row and get `None`. Cross-process safety is reinforced with a revision-guarded
    /// `UPDATE` as a second line of defence.
    pub fn claim_next(
        &self,
        destination_remote: &str,
        worker: &str,
        ttl: Duration,
    ) -> Result<Option<OutboxOperation>> {
        validate_actor(worker)?;
        if ttl <= Duration::ZERO {
            return Err(StorageError::Invariant("lease ttl must be positive".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = OffsetDateTime::now_utc();
        // Candidate heads only: `NOT EXISTS` an earlier active-delivery predecessor in the same
        // (destination_remote, ordering_key) group. Terminal states do not block; staged rows are
        // not candidates but do block later operations until reconciliation settles them.
        // `retry_after` eligibility is decided in
        // Rust after parsing, never by SQL string comparison: RFC 3339 fractional-second widths
        // differ between rows, which would make a pure lexicographic `<=` unreliable. No LIMIT is
        // applied: skipping any head could report "no work" while an eligible due head exists.
        let due = {
            let mut statement = tx.prepare(
                "SELECT c.operation_id, c.revision, c.retry_after FROM replication_outbox c \
                 WHERE c.destination_remote=?1 AND c.state IN ('pending','retryable') \
                 AND NOT EXISTS (SELECT 1 FROM replication_outbox p \
                                  WHERE p.destination_remote=c.destination_remote \
                                    AND p.ordering_key=c.ordering_key \
                                    AND p.sequence<c.sequence \
                                    AND p.state IN ('staged','pending','leased','retryable')) \
                 ORDER BY c.sequence",
            )?;
            let rows = statement
                .query_map([destination_remote], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows.into_iter()
                .find(|(_, _, retry_after)| match retry_after {
                    Some(when) => parse_time(when.clone()).map(|when| when <= now).unwrap_or(false),
                    None => true,
                })
                .map(|(operation_id, revision, _)| (operation_id, revision))
        };
        let Some((operation_id, revision)) = due else {
            tx.commit()?;
            return Ok(None);
        };
        let expiry = now
            .checked_add(ttl)
            .ok_or_else(|| StorageError::Invariant(OUTBOX_LEASE_DURATION_OUT_OF_RANGE.into()))?;
        let changed = tx.execute(
            "UPDATE replication_outbox SET state='leased',lease_owner=?3,lease_expires_at=?4,\
             retry_after=NULL,updated_at=?5,revision=revision+1 WHERE operation_id=?1 AND revision=?2",
            params![operation_id, revision, worker, format_time(expiry)?, format_time(now)?],
        )?;
        if changed != 1 {
            // Lost a race against another writer inside the same transaction — not expected in
            // practice (the transaction mode serializes writers) but handled defensively.
            tx.commit()?;
            return Ok(None);
        }
        let op = get_operation_tx(&tx, &operation_id)?
            .ok_or_else(|| StorageError::Invariant("claimed operation disappeared".into()))?;
        tx.commit()?;
        Ok(Some(op))
    }

    /// Claim one specific operation using compare-and-swap revision semantics.
    ///
    /// A stale/absent `expected_revision` returns `Ok(`[`RevisionedWrite::Conflict`]`)`. A
    /// staged operation ([`OUTBOX_OPERATION_NOT_ACTIVATED`]), a blocked one with a live
    /// predecessor in its group ([`OUTBOX_PREDECESSOR_IN_FLIGHT`]), a live lease held by another
    /// worker, a terminal operation, or a retry that is not yet due remain `Err` built from the
    /// `OUTBOX_*` constants. An expired lease is recovered here just like
    /// [`crate::coordination`]'s `claim_task`: the caller of the expired lease — or any worker on
    /// an expired lease — may claim and refresh it.
    pub fn claim_by_id(
        &self,
        operation_id: &str,
        worker: &str,
        expected_revision: i64,
        ttl: Duration,
    ) -> Result<RevisionedWrite<OutboxOperation>> {
        validate_actor(worker)?;
        if ttl <= Duration::ZERO {
            return Err(StorageError::Invariant("lease ttl must be positive".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(op) = get_operation_tx(&tx, operation_id)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        let now = OffsetDateTime::now_utc();
        if op.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(op.revision) });
        }
        if op.state == OutboxState::Staged {
            return Err(StorageError::Invariant(OUTBOX_OPERATION_NOT_ACTIVATED.into()));
        }
        if op.state.terminal() {
            return Err(StorageError::Invariant(OUTBOX_OPERATION_TERMINAL.into()));
        }
        // Head-of-line: an earlier nonterminal operation in the same group blocks this one, the
        // same rule claim_next enforces. Applied unconditionally — an op in flight was itself a
        // group head when claimed, so it never has a live predecessor.
        if has_predecessor(&tx, &op.destination_remote, &op.ordering_key, op.sequence)? {
            return Err(StorageError::Invariant(OUTBOX_PREDECESSOR_IN_FLIGHT.into()));
        }
        if op.retry_after.is_some_and(|when| when > now) {
            return Err(StorageError::Invariant(OUTBOX_RETRY_NOT_DUE.into()));
        }
        if op.state == OutboxState::Leased
            && op.lease_owner.as_deref().is_some_and(|owner| owner != worker)
            && op.lease_expires_at.is_some_and(|expiry| expiry > now)
        {
            return Err(StorageError::Invariant(OUTBOX_LEASE_HELD_BY_ANOTHER_WORKER.into()));
        }
        let expiry = now
            .checked_add(ttl)
            .ok_or_else(|| StorageError::Invariant(OUTBOX_LEASE_DURATION_OUT_OF_RANGE.into()))?;
        let changed = tx.execute(
            "UPDATE replication_outbox SET state='leased',lease_owner=?3,lease_expires_at=?4,\
             retry_after=NULL,updated_at=?5,revision=revision+1 WHERE operation_id=?1 AND revision=?2",
            params![operation_id, expected_revision, worker, format_time(expiry)?, format_time(now)?],
        )?;
        if changed != 1 {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        }
        let result = get_operation_tx(&tx, operation_id)?
            .ok_or_else(|| StorageError::Invariant("claimed operation disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Renew a live lease using compare-and-swap revision semantics.
    ///
    /// See [`Self::claim_by_id`] for the revision-vs-state conflict split: a stale revision
    /// returns `Ok(Conflict)`; not holding the lease, or the lease having already expired, remain
    /// `Err`.
    pub fn renew_lease(
        &self,
        operation_id: &str,
        worker: &str,
        expected_revision: i64,
        ttl: Duration,
    ) -> Result<RevisionedWrite<OutboxOperation>> {
        validate_actor(worker)?;
        if ttl <= Duration::ZERO {
            return Err(StorageError::Invariant("lease ttl must be positive".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(op) = get_operation_tx(&tx, operation_id)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        let now = OffsetDateTime::now_utc();
        if op.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(op.revision) });
        }
        if op.state != OutboxState::Leased || op.lease_owner.as_deref() != Some(worker) {
            return Err(StorageError::Invariant(OUTBOX_ONLY_LEASE_OWNER.into()));
        }
        if op.lease_expires_at.is_none_or(|expiry| expiry <= now) {
            return Err(StorageError::Invariant(OUTBOX_LEASE_HAS_EXPIRED.into()));
        }
        let next = op.revision + 1;
        let expiry = now
            .checked_add(ttl)
            .ok_or_else(|| StorageError::Invariant(OUTBOX_LEASE_DURATION_OUT_OF_RANGE.into()))?;
        tx.execute(
            "UPDATE replication_outbox SET lease_expires_at=?2,revision=?3,updated_at=?4 \
             WHERE operation_id=?1",
            params![operation_id, format_time(expiry)?, next, format_time(now)?],
        )?;
        let result = get_operation_tx(&tx, operation_id)?
            .ok_or_else(|| StorageError::Invariant("renewed operation disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Acknowledge an in-flight operation as delivered, moving it to the terminal
    /// [`OutboxState::Replicated`] state.
    ///
    /// Only the lease owner of a live lease may acknowledge. Acknowledging an already-replicated
    /// operation with its current revision is a harmless no-op (it returns the operation as-is),
    /// which makes at-least-once delivery acknowledgements idempotent.
    pub fn acknowledge(
        &self,
        operation_id: &str,
        worker: &str,
        expected_revision: i64,
    ) -> Result<RevisionedWrite<OutboxOperation>> {
        validate_actor(worker)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(op) = get_operation_tx(&tx, operation_id)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        let now = OffsetDateTime::now_utc();
        if op.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(op.revision) });
        }
        if op.state == OutboxState::Replicated {
            tx.commit()?;
            return Ok(RevisionedWrite::Applied(op));
        }
        ensure_in_flight(&op, worker, now)?;
        let next = op.revision + 1;
        tx.execute(
            "UPDATE replication_outbox SET state='replicated',revision=?2,lease_owner=NULL,\
             lease_expires_at=NULL,retry_after=NULL,last_error=NULL,updated_at=?3 WHERE operation_id=?1",
            params![operation_id, next, format_time(now)?],
        )?;
        let result = get_operation_tx(&tx, operation_id)?
            .ok_or_else(|| StorageError::Invariant("acknowledged operation disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Record a failed in-flight attempt as retryable.
    ///
    /// Advances `attempt_count` by one and moves the operation to [`OutboxState::Retryable`],
    /// which cannot be claimed again before `retry_after`. Remote outages can last arbitrarily
    /// long, so a transport/unknown failure stays retryable **indefinitely**: `max_attempts` is
    /// advisory only and never exhausts into terminal [`OutboxState::Failed`]. Terminal failure is
    /// reserved for [`Self::fail`], which callers use for an authoritative permanent error. The
    /// retry policy (how far out `retry_after` should be) lives with the caller; the store only
    /// records and respects it. The lease is released.
    pub fn schedule_retry(
        &self,
        operation_id: &str,
        worker: &str,
        expected_revision: i64,
        error: &str,
        retry_after: OffsetDateTime,
    ) -> Result<RevisionedWrite<OutboxOperation>> {
        validate_actor(worker)?;
        bounded_error(error)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(op) = get_operation_tx(&tx, operation_id)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        let now = OffsetDateTime::now_utc();
        if op.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(op.revision) });
        }
        ensure_in_flight(&op, worker, now)?;
        let next = op.revision + 1;
        let attempts = op.attempt_count + 1;
        tx.execute(
            "UPDATE replication_outbox SET state='retryable',revision=?2,lease_owner=NULL,\
             lease_expires_at=NULL,retry_after=?3,attempt_count=?4,last_error=?5,updated_at=?6 \
             WHERE operation_id=?1",
            params![
                operation_id,
                next,
                Some(format_time(retry_after)?),
                attempts,
                error,
                format_time(now)?,
            ],
        )?;
        let result = get_operation_tx(&tx, operation_id)?
            .ok_or_else(|| StorageError::Invariant("retried operation disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Fail an in-flight operation terminally, moving it to [`OutboxState::Failed`] regardless of
    /// how many attempts remain.
    ///
    /// Intended for non-retryable delivery errors (e.g. the remote rejected the operation
    /// deterministically). Releases the lease and records the error.
    pub fn fail(
        &self,
        operation_id: &str,
        worker: &str,
        expected_revision: i64,
        error: &str,
    ) -> Result<RevisionedWrite<OutboxOperation>> {
        validate_actor(worker)?;
        bounded_error(error)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(op) = get_operation_tx(&tx, operation_id)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        let now = OffsetDateTime::now_utc();
        if op.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(op.revision) });
        }
        ensure_in_flight(&op, worker, now)?;
        let next = op.revision + 1;
        let attempts = op.attempt_count + 1;
        tx.execute(
            "UPDATE replication_outbox SET state='failed',revision=?2,lease_owner=NULL,\
             lease_expires_at=NULL,retry_after=NULL,attempt_count=?3,last_error=?4,updated_at=?5 \
             WHERE operation_id=?1",
            params![operation_id, next, attempts, error, format_time(now)?],
        )?;
        let result = get_operation_tx(&tx, operation_id)?
            .ok_or_else(|| StorageError::Invariant("failed operation disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Claim the oldest leased operation whose lease has already expired, if any.
    ///
    /// Lease-expiry recovery for the batch claimer: `claim_next` routes only `pending`/`retryable`
    /// rows, so a worker that died mid-flight leaves its operation leased forever unless something
    /// reclaims it. This finds the oldest such expired lease (optionally scoped to one remote) and
    /// hands it to `worker` with a fresh lease.
    pub fn reclaim_expired_lease(
        &self,
        destination_remote: Option<&str>,
        worker: &str,
        ttl: Duration,
    ) -> Result<Option<OutboxOperation>> {
        validate_actor(worker)?;
        if ttl <= Duration::ZERO {
            return Err(StorageError::Invariant("lease ttl must be positive".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = OffsetDateTime::now_utc();
        let operation_id: Option<String> = match destination_remote {
            Some(remote) => tx
                .query_row(
                    "SELECT operation_id FROM replication_outbox WHERE destination_remote=?1 AND \
                     state='leased' AND lease_expires_at IS NOT NULL AND \
                     julianday(lease_expires_at)<=julianday(?2) ORDER BY sequence LIMIT 1",
                    params![remote, format_time(now)?],
                    |row| row.get(0),
                )
                .optional()?,
            None => tx
                .query_row(
                    "SELECT operation_id FROM replication_outbox WHERE state='leased' AND \
                     lease_expires_at IS NOT NULL AND julianday(lease_expires_at)<=julianday(?1) \
                     ORDER BY sequence LIMIT 1",
                    params![format_time(now)?],
                    |row| row.get(0),
                )
                .optional()?,
        };
        let Some(operation_id) = operation_id else {
            tx.commit()?;
            return Ok(None);
        };
        let expiry = now
            .checked_add(ttl)
            .ok_or_else(|| StorageError::Invariant(OUTBOX_LEASE_DURATION_OUT_OF_RANGE.into()))?;
        let changed = tx.execute(
            "UPDATE replication_outbox SET lease_owner=?2,lease_expires_at=?3,updated_at=?4,\
             revision=revision+1 WHERE operation_id=?1",
            params![operation_id, worker, format_time(expiry)?, format_time(now)?],
        )?;
        if changed != 1 {
            tx.commit()?;
            return Ok(None);
        }
        let op = get_operation_tx(&tx, &operation_id)?
            .ok_or_else(|| StorageError::Invariant("reclaimed operation disappeared".into()))?;
        tx.commit()?;
        Ok(Some(op))
    }

    /// Backlog/status summary of the outbox, optionally scoped to one destination remote.
    pub fn backlog(&self, destination_remote: Option<&str>) -> Result<OutboxBacklog> {
        let conn = self.connection()?;
        let scoped = destination_remote.is_some();
        let pending_summary = if let Some(remote) = destination_remote {
            conn.query_row(
                "SELECT COUNT(*), MIN(julianday(created_at)) FROM replication_outbox \
                 WHERE state='pending' AND destination_remote=?1",
                [remote],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<f64>>(1)?)),
            )?
        } else {
            conn.query_row(
                "SELECT COUNT(*), MIN(julianday(created_at)) FROM replication_outbox \
                 WHERE state='pending'",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<f64>>(1)?)),
            )?
        };
        let oldest_pending_at = match pending_summary.1 {
            Some(julian) => Some(julianday_to_time(julian)?),
            None => None,
        };
        let counts = {
            let mut bindings: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            if let Some(remote) = destination_remote {
                bindings.push(Box::new(remote.to_owned()));
            }
            let placeholder = scoped.then(|| format!("destination_remote=?{}", bindings.len()));
            let mut sql = "SELECT \
                COUNT(*) FILTER (WHERE state='pending'), \
                COUNT(*) FILTER (WHERE state='staged'), \
                COUNT(*) FILTER (WHERE state='leased'), \
                COUNT(*) FILTER (WHERE state='retryable'), \
                COUNT(*) FILTER (WHERE state='failed'), \
                COUNT(*) FILTER (WHERE state='cancelled'), \
                COUNT(*) FROM replication_outbox"
                .to_owned();
            if let Some(cond) = placeholder {
                sql.push_str(" WHERE ");
                sql.push_str(&cond);
            }
            let mut statement = conn.prepare(&sql)?;
            let parameters =
                bindings.iter().map(AsRef::as_ref).collect::<Vec<&dyn rusqlite::ToSql>>();
            statement.query_row(parameters.as_slice(), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })?
        };
        // The oldest retryable operation by earliest retry_after: its attempt count and last error
        // are the "retry count / last error" an operator most needs first.
        let (retry_attempt_count, retry_last_error) = if let Some(remote) = destination_remote {
            conn.query_row(
                "SELECT attempt_count, last_error FROM replication_outbox WHERE state='retryable' \
                 AND destination_remote=?1 ORDER BY julianday(retry_after), sequence LIMIT 1",
                [remote],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?
        } else {
            conn.query_row(
                "SELECT attempt_count, last_error FROM replication_outbox WHERE state='retryable' \
                 ORDER BY julianday(retry_after), sequence LIMIT 1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
            )
            .optional()?
        }
        .unwrap_or((0, None));
        Ok(OutboxBacklog {
            destination_remote: destination_remote.map(ToOwned::to_owned),
            pending_count: pending_summary.0,
            staged_count: counts.1,
            oldest_pending_at,
            leased_count: counts.2,
            retryable_count: counts.3,
            retry_attempt_count,
            retry_last_error,
            failed_count: counts.4,
            cancelled_count: counts.5,
            total_count: counts.6,
        })
    }

    fn connection(&self) -> Result<Connection> {
        crate::coordination::open_palace_connection(&self.path)
    }
}

const OPERATION_COLUMNS: &str = "SELECT operation_id,sequence,created_by,idempotency_key,mutation_kind,\
entity_id,destination_remote,ordering_key,entity_sequence,state,revision,lease_owner,\
lease_expires_at,attempt_count,max_attempts,retry_after,last_error,payload_json,created_at,updated_at \
FROM replication_outbox";

/// Shared in-flight guard for the lease-mutating transitions: the operation must be currently
/// leased, by this worker, on a live lease. Revision mismatches are handled before this is called.
fn ensure_in_flight(op: &OutboxOperation, worker: &str, now: OffsetDateTime) -> Result<()> {
    if op.state != OutboxState::Leased {
        return Err(StorageError::Invariant(OUTBOX_OPERATION_NOT_IN_FLIGHT.into()));
    }
    if op.lease_owner.as_deref() != Some(worker) {
        return Err(StorageError::Invariant(OUTBOX_ONLY_LEASE_OWNER.into()));
    }
    if op.lease_expires_at.is_none_or(|expiry| expiry <= now) {
        return Err(StorageError::Invariant(OUTBOX_LEASE_HAS_EXPIRED.into()));
    }
    Ok(())
}

/// Whether an earlier operation currently active for delivery shares `candidate`'s
/// `(destination_remote, ordering_key)` with a smaller `sequence`. Terminal operations
/// (replicated, failed, cancelled) never block. Staged operations are not claimable, but remain
/// barriers until activated or cancelled.
fn has_predecessor(
    conn: &Connection,
    destination_remote: &str,
    ordering_key: &str,
    sequence: i64,
) -> Result<bool> {
    let exists: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM replication_outbox p \
         WHERE p.destination_remote=?1 AND p.ordering_key=?2 AND p.sequence<?3 \
           AND p.state IN ('staged','pending','leased','retryable'))",
        params![destination_remote, ordering_key, sequence],
        |row| row.get(0),
    )?;
    Ok(exists)
}

fn get_operation_conn(conn: &Connection, id: &str) -> Result<Option<OutboxOperation>> {
    conn.prepare(&format!("{OPERATION_COLUMNS} WHERE operation_id=?1"))?
        .query_row([id], operation_row)
        .optional()
        .map_err(Into::into)
}
fn get_operation_tx(tx: &Transaction<'_>, id: &str) -> Result<Option<OutboxOperation>> {
    get_operation_conn(tx, id)
}
fn find_operation_by_key(
    tx: &Transaction<'_>,
    actor: &str,
    key: &str,
) -> Result<Option<OutboxOperation>> {
    let id: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM replication_outbox WHERE created_by=?1 AND idempotency_key=?2",
            params![actor, key],
            |row| row.get(0),
        )
        .optional()?;
    id.map(|value| {
        get_operation_tx(tx, &value)?
            .ok_or_else(|| StorageError::Invariant("operation key points to missing row".into()))
    })
    .transpose()
}
fn operation_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxOperation> {
    let state: String = row.get(9)?;
    let lease_expires: Option<String> = row.get(12)?;
    let retry_after: Option<String> = row.get(15)?;
    let payload: String = row.get(17)?;
    let created: String = row.get(18)?;
    let updated: String = row.get(19)?;
    Ok(OutboxOperation {
        operation_id: row.get(0)?,
        sequence: row.get(1)?,
        created_by: row.get(2)?,
        idempotency_key: row.get(3)?,
        mutation_kind: row.get(4)?,
        entity_id: row.get(5)?,
        destination_remote: row.get(6)?,
        ordering_key: row.get(7)?,
        entity_sequence: row.get(8)?,
        state: OutboxState::parse(&state).map_err(sql_conv)?,
        revision: row.get(10)?,
        lease_owner: row.get(11)?,
        lease_expires_at: parse_time_opt(lease_expires).map_err(sql_conv)?,
        attempt_count: row.get(13)?,
        max_attempts: row.get(14)?,
        retry_after: parse_time_opt(retry_after).map_err(sql_conv)?,
        last_error: row.get(16)?,
        payload: serde_json::from_str(&payload).map_err(sql_conv)?,
        created_at: parse_time(created).map_err(sql_conv)?,
        updated_at: parse_time(updated).map_err(sql_conv)?,
    })
}

fn validate_actor(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant("actor must not be empty".into()))
    } else {
        Ok(())
    }
}
fn validate_key(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
        Err(StorageError::Invariant("idempotency key must contain 1..=256 bytes".into()))
    } else {
        Ok(())
    }
}
fn bounded_identifier(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant(format!("{name} must not be empty")))
    } else if value.len() > MAX_IDENTIFIER_BYTES {
        Err(StorageError::Invariant(format!("{name} must be at most {MAX_IDENTIFIER_BYTES} bytes")))
    } else {
        Ok(())
    }
}
fn nonempty_identifier(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant(format!("{name} must not be empty")))
    } else {
        Ok(())
    }
}
fn bounded_json(value: &Value) -> Result<()> {
    if serde_json::to_vec(value)?.len() > MAX_PAYLOAD_BYTES {
        Err(StorageError::Invariant("payload exceeds 16 MiB".into()))
    } else {
        Ok(())
    }
}
fn bounded_error(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant("error must not be empty".into()))
    } else if value.len() > MAX_ERROR_BYTES {
        Err(StorageError::Invariant(format!("error exceeds {MAX_ERROR_BYTES} bytes")))
    } else {
        Ok(())
    }
}
fn format_time(value: OffsetDateTime) -> Result<String> {
    value.format(&Rfc3339).map_err(|error| StorageError::Invariant(error.to_string()))
}
fn parse_time(value: String) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(&value, &Rfc3339)
        .map_err(|error| StorageError::Invariant(error.to_string()))
}
fn parse_time_opt(value: Option<String>) -> Result<Option<OffsetDateTime>> {
    value.map(parse_time).transpose()
}
/// Convert a SQLite `julianday` value to an `OffsetDateTime`. `julianday` is a fractional day
/// count since 4713 BC, so the Unix epoch is day 2440587.5; this conversion is second-precision.
fn julianday_to_time(value: f64) -> Result<OffsetDateTime> {
    const UNIX_EPOCH_JULIAN_DAY: f64 = 2_440_587.5;
    let unix_seconds = ((value - UNIX_EPOCH_JULIAN_DAY) * 86_400.0) as i64;
    OffsetDateTime::from_unix_timestamp(unix_seconds)
        .map_err(|error| StorageError::Invariant(error.to_string()))
}
fn sql_conv<E: std::error::Error + Send + Sync + 'static>(error: E) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store() -> (TempDir, OutboxStore) {
        let dir = TempDir::new().expect("temp dir");
        let outbox = OutboxStore::new(dir.path().join("storage.sqlite3"));
        outbox.ensure_schema().expect("schema");
        (dir, outbox)
    }

    fn enqueue(outbox: &OutboxStore, entity: &str, key: &str, op_key: &str) -> OutboxOperation {
        outbox
            .enqueue(&NewOutboxOperation {
                created_by: "repl-worker".into(),
                idempotency_key: op_key.into(),
                mutation_kind: "drawer_added".into(),
                entity_id: entity.into(),
                destination_remote: "actuarius".into(),
                ordering_key: key.into(),
                payload: serde_json::json!({"entity": entity, "ordering_key": key}),
                max_attempts: 3,
            })
            .expect("enqueue")
    }

    fn activate(outbox: &OutboxStore, op: &OutboxOperation) -> OutboxOperation {
        expect_applied(outbox.activate(&op.operation_id, op.revision).expect("activate"))
    }

    /// Enqueue a staged intent and immediately confirm its local commit, returning a deliverable
    /// pending operation. Used by tests that are not about staging itself.
    fn enqueue_active(
        outbox: &OutboxStore,
        entity: &str,
        key: &str,
        op_key: &str,
    ) -> OutboxOperation {
        let staged = enqueue(outbox, entity, key, op_key);
        activate(outbox, &staged)
    }

    fn expect_applied(write: RevisionedWrite<OutboxOperation>) -> OutboxOperation {
        match write {
            RevisionedWrite::Applied(op) => op,
            RevisionedWrite::Conflict { actual_revision } => {
                panic!(
                    "expected an applied write, got a conflict (actual_revision={actual_revision:?})"
                )
            }
        }
    }

    fn expect_invariant(err: &StorageError) -> &str {
        match err {
            StorageError::Invariant(msg) => msg.as_str(),
            other => panic!("expected StorageError::Invariant, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_enqueue_replays_the_same_operation() {
        let (_dir, outbox) = store();
        let first = enqueue(&outbox, "entity-a", "k1", "op-1");
        assert_eq!(first.state, OutboxState::Staged, "an enqueue lands in the staged state");
        let replay = outbox
            .enqueue(&NewOutboxOperation {
                created_by: "repl-worker".into(),
                idempotency_key: "op-1".into(),
                mutation_kind: "drawer_added".into(),
                entity_id: "entity-a".into(),
                destination_remote: "actuarius".into(),
                ordering_key: "k1".into(),
                payload: serde_json::json!({"entity": "entity-a", "ordering_key": "k1"}),
                max_attempts: 3,
            })
            .expect("replay");
        assert_eq!(first, replay, "a replay must return the committed operation untouched");
        assert_eq!(outbox.get_operation(&first.operation_id).expect("get"), Some(first.clone()));
        assert_eq!(first.entity_sequence, 1);
        // A distinct key on the same entity advances the sequence; a replay of the first does not.
        let second = enqueue(&outbox, "entity-a", "k2", "op-2");
        assert_eq!(second.entity_sequence, 2);
        assert!(outbox.get_operation("outbox_missing").expect("miss").is_none());
    }

    #[test]
    fn find_by_key_recovers_an_operation_by_caller_key_across_states() {
        let (_dir, outbox) = store();
        // A fresh key with no committed operation is an authoritative miss.
        assert!(
            outbox.find_by_key("repl-worker", "op-missing").expect("miss").is_none(),
            "find_by_key must return None for an unknown key"
        );
        let staged = enqueue(&outbox, "entity-a", "k1", "op-key-1");
        let found_staged =
            outbox.find_by_key("repl-worker", "op-key-1").expect("find staged").expect("some");
        assert_eq!(found_staged, staged, "find_by_key must recover the staged operation");
        assert_eq!(
            found_staged.state,
            OutboxState::Staged,
            "recovered operation keeps its staged state"
        );
        // The recovered operation is a live handle: it can be activated through its own id.
        let pending = activate(&outbox, &found_staged);
        assert_eq!(pending.state, OutboxState::Pending);
        let found_pending =
            outbox.find_by_key("repl-worker", "op-key-1").expect("find pending").expect("some");
        assert_eq!(found_pending, pending, "find_by_key reflects the activated state");
        // Scope by created_by: another actor with the same key is a miss.
        assert!(
            outbox.find_by_key("other-actor", "op-key-1").expect("actor miss").is_none(),
            "find_by_key is scoped to created_by"
        );
    }

    #[test]
    fn idempotency_key_reuse_with_a_different_mutation_is_rejected() {
        let (_dir, outbox) = store();
        let first = enqueue(&outbox, "entity-a", "k1", "op-1");
        for (entity_id, remote, payload) in [
            ("entity-b", "actuarius", serde_json::json!({"entity": "entity-b"})),
            ("entity-a", "elsewhere", serde_json::json!({"entity": "entity-a"})),
            ("entity-a", "actuarius", serde_json::json!({"changed": true})),
        ] {
            let error = outbox
                .enqueue(&NewOutboxOperation {
                    created_by: "repl-worker".into(),
                    idempotency_key: "op-1".into(),
                    mutation_kind: "drawer_added".into(),
                    entity_id: entity_id.into(),
                    destination_remote: remote.into(),
                    ordering_key: "k1".into(),
                    payload,
                    max_attempts: 3,
                })
                .expect_err("a key cannot name two different mutations");
            assert!(expect_invariant(&error).contains("reused with a different mutation"));
        }
        assert_eq!(outbox.get_operation(&first.operation_id).expect("get"), Some(first));
    }

    #[test]
    fn staged_operations_are_not_claimable_but_block_later_same_group() {
        let (_dir, outbox) = store();
        let staged = enqueue(&outbox, "entity-a", "g", "op-staged");
        // Neither the batch claimer nor the exact claimer may deliver a staged operation.
        assert!(
            outbox
                .claim_next("actuarius", "worker-a", Duration::minutes(1))
                .expect("claim_next")
                .is_none(),
            "claim_next must never return a staged operation"
        );
        let err = outbox
            .claim_by_id(&staged.operation_id, "worker-a", staged.revision, Duration::minutes(1))
            .expect_err("staged claim must fail");
        assert!(expect_invariant(&err).starts_with(OUTBOX_OPERATION_NOT_ACTIVATED), "{err}");

        // A staged operation remains an ordering barrier for its own group: startup
        // reconciliation must settle the staged intent before the later operation can deliver.
        let later = enqueue_active(&outbox, "entity-a", "g", "op-later");
        assert_eq!(
            outbox
                .claim_next("actuarius", "worker-a", Duration::minutes(1))
                .expect("claim_next")
                .as_ref()
                .map(|op| &op.operation_id),
            None,
            "a staged predecessor must block an activated later operation in its group"
        );
        // Another group is unaffected (sanity — no shared blocking state).
        let other = enqueue_active(&outbox, "entity-b", "h", "op-other");
        assert_eq!(
            outbox
                .claim_next("actuarius", "worker-a", Duration::minutes(1))
                .expect("claim_next")
                .expect("other")
                .operation_id,
            other.operation_id,
            "unrelated groups stay independent"
        );

        // Once reconciliation activates the staged predecessor, it is claimable first; the later
        // operation remains behind it until that predecessor is acknowledged.
        let activated = expect_applied(
            outbox.activate(&staged.operation_id, staged.revision).expect("activate staged"),
        );
        assert_eq!(
            outbox
                .claim_next("actuarius", "worker-a", Duration::minutes(1))
                .expect("claim_next")
                .expect("activated predecessor")
                .operation_id,
            activated.operation_id
        );
        let blocked = outbox
            .claim_by_id(&later.operation_id, "worker-b", later.revision, Duration::minutes(1))
            .expect_err("later operation remains blocked");
        assert_eq!(expect_invariant(&blocked), OUTBOX_PREDECESSOR_IN_FLIGHT);
    }

    #[test]
    fn activation_and_cancellation_are_cas_transitions() {
        let (_dir, outbox) = store();
        let staged = enqueue(&outbox, "entity-a", "g", "op-staged");

        // Activation is a CAS transition: a stale revision is a typed conflict.
        let stale =
            outbox.activate(&staged.operation_id, staged.revision + 7).expect("activate call");
        assert!(matches!(stale, RevisionedWrite::Conflict { .. }));

        // Activating the staged intent works and the operation is no longer stageable.
        let activated = expect_applied(
            outbox.activate(&staged.operation_id, staged.revision).expect("activate staged"),
        );
        assert_eq!(activated.state, OutboxState::Pending);
        let err = outbox
            .activate(&activated.operation_id, activated.revision)
            .expect_err("re-activating a pending operation must fail");
        assert!(expect_invariant(&err).starts_with(OUTBOX_ONLY_STAGED_MAY_ACTIVATE), "{err}");

        // A pending (activated) operation cannot be cancelled: the local commit is confirmed, so
        // the durable replication obligation must not be silently abandoned.
        let err = outbox
            .cancel(&activated.operation_id, activated.revision)
            .expect_err("a pending operation must not be cancelled");
        assert!(expect_invariant(&err).starts_with(OUTBOX_CANCELLABLE_STATE_REQUIRED), "{err}");

        // Cancelling a *staged* intent is terminal and unclaimable.
        let staged2 = enqueue(&outbox, "entity-b", "h", "op-staged2");
        let cancelled = expect_applied(
            outbox.cancel(&staged2.operation_id, staged2.revision).expect("cancel staged"),
        );
        assert_eq!(cancelled.state, OutboxState::Cancelled);
        // The activated op from earlier is the only remaining deliverable; retire it, then the
        // cancelled op is the only row and must not surface.
        let only_pending = outbox
            .claim_next("actuarius", "worker-a", Duration::minutes(1))
            .expect("claim_next")
            .expect("pending");
        assert_eq!(only_pending.operation_id, activated.operation_id);
        expect_applied(
            outbox
                .acknowledge(&only_pending.operation_id, "worker-a", only_pending.revision)
                .expect("ack"),
        );
        assert!(
            outbox
                .claim_next("actuarius", "worker-a", Duration::minutes(1))
                .expect("claim_next")
                .is_none(),
            "a cancelled operation must not be returned by the batch claimer"
        );
        let err = outbox
            .claim_by_id(
                &staged2.operation_id,
                "worker-a",
                cancelled.revision,
                Duration::minutes(1),
            )
            .expect_err("a cancelled operation must not be claimable");
        assert!(expect_invariant(&err).starts_with(OUTBOX_OPERATION_TERMINAL), "{err}");
    }

    #[test]
    fn retry_cancelled_staged_operation_becomes_deliverable_again() {
        let (_dir, outbox) = store();
        let input = NewOutboxOperation {
            created_by: "worker".into(),
            idempotency_key: "retry-cancel-key".into(),
            mutation_kind: "drawer_added".into(),
            entity_id: "drawer_abc".into(),
            destination_remote: "actuarius".into(),
            ordering_key: "drawer_abc".into(),
            payload: serde_json::json!({"content": "initial"}),
            max_attempts: 5,
        };
        let staged = outbox.enqueue(&input).expect("enqueue");
        assert_eq!(staged.state, OutboxState::Staged);
        assert_eq!(staged.revision, 0);

        // Cancel the staged intent (local mutation aborted).
        let cancelled = expect_applied(
            outbox.cancel(&staged.operation_id, staged.revision).expect("cancel staged"),
        );
        assert_eq!(cancelled.state, OutboxState::Cancelled);
        assert_eq!(cancelled.revision, 1);

        // Attempting to re-enqueue with a different payload is rejected.
        let mut different_input = input.clone();
        different_input.payload = serde_json::json!({"content": "different"});
        let err = outbox.enqueue(&different_input).expect_err("reused key with different payload");
        assert!(err.to_string().contains("reused with a different mutation"));

        // Retrying with the same operation/payload revives the intent to staged.
        let revived = outbox.enqueue(&input).expect("revive cancelled enqueue");
        assert_eq!(revived.operation_id, staged.operation_id);
        assert_eq!(revived.state, OutboxState::Staged);
        assert_eq!(revived.revision, 2);

        // Revived staged operation can now be activated after local commit succeeds.
        let activated = expect_applied(
            outbox.activate(&revived.operation_id, revived.revision).expect("activate revived"),
        );
        assert_eq!(activated.state, OutboxState::Pending);

        // Activated operation is claimed and delivered normally.
        let claimed = outbox
            .claim_next("actuarius", "worker", Duration::minutes(1))
            .expect("claim")
            .expect("found pending");
        assert_eq!(claimed.operation_id, staged.operation_id);
        let acked = expect_applied(
            outbox.acknowledge(&claimed.operation_id, "worker", claimed.revision).expect("ack"),
        );
        assert_eq!(acked.state, OutboxState::Replicated);
    }

    #[test]
    fn staged_state_survives_reopen() {
        let (dir, outbox) = store();
        let staged = enqueue(&outbox, "entity-a", "g", "op-staged");
        drop(outbox);

        let reopened = OutboxStore::new(dir.path().join("storage.sqlite3"));
        reopened.ensure_schema().expect("schema on reopen");
        let reloaded = reopened.get_operation(&staged.operation_id).expect("get").expect("present");
        assert_eq!(reloaded.state, OutboxState::Staged, "staged intent must survive a restart");
        let listed = reopened.list_staged(10).expect("list staged");
        assert_eq!(listed, vec![reloaded.clone()]);
        assert!(
            reopened
                .claim_next("actuarius", "worker-a", Duration::minutes(1))
                .expect("claim_next")
                .is_none(),
            "a staged operation must not become claimable after reopen"
        );
        // Reconciliation can still decide it after reopen.
        let active = activate(&reopened, &reloaded);
        assert_eq!(active.state, OutboxState::Pending);
    }

    #[test]
    fn concurrent_claimers_cannot_both_claim_the_same_operation() {
        let (dir, outbox) = store();
        let op = enqueue_active(&outbox, "entity-a", "g", "op-1");
        let revision = op.revision;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let mut joins = Vec::new();
        for worker in ["worker-a", "worker-b"] {
            let path = dir.path().join("storage.sqlite3");
            let operation_id = op.operation_id.clone();
            let gate = barrier.clone();
            joins.push(std::thread::spawn(move || {
                let store = OutboxStore::new(path);
                gate.wait();
                // Exactly one worker may observe its claim as `Applied`; the loser observes a
                // typed conflict.
                matches!(
                    store
                        .claim_by_id(&operation_id, worker, revision, Duration::minutes(1))
                        .expect("claim call"),
                    RevisionedWrite::Applied(_)
                )
            }));
        }
        barrier.wait();
        let wins = joins
            .into_iter()
            .map(|join| join.join().expect("worker thread"))
            .filter(|won| *won)
            .count();
        assert_eq!(wins, 1, "exactly one concurrent claimant may win");
        let claimed = outbox.get_operation(&op.operation_id).expect("get").expect("present");
        assert_eq!(claimed.state, OutboxState::Leased);
        assert!(claimed.lease_owner.is_some());
    }

    #[test]
    fn expired_lease_is_reclaimable_by_another_worker() {
        let (_dir, outbox) = store();
        let op = enqueue_active(&outbox, "entity-a", "g", "op-1");
        let claimed = expect_applied(
            outbox
                .claim_by_id(&op.operation_id, "worker-a", op.revision, Duration::milliseconds(1))
                .expect("claim"),
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
        let reclaimed = expect_applied(
            outbox
                .claim_by_id(&op.operation_id, "worker-b", claimed.revision, Duration::minutes(1))
                .expect("reclaim"),
        );
        assert_eq!(reclaimed.lease_owner.as_deref(), Some("worker-b"));
        assert_eq!(reclaimed.revision, claimed.revision + 1);
    }

    #[test]
    fn reclaim_expired_lease_recovers_orphaned_in_flight_operations() {
        let (_dir, outbox) = store();
        let op = enqueue_active(&outbox, "entity-a", "g", "op-1");
        let claimed = expect_applied(
            outbox
                .claim_by_id(&op.operation_id, "worker-a", op.revision, Duration::seconds(2))
                .expect("claim"),
        );
        // While worker-a's lease is still live, nothing is expired to reclaim.
        assert!(
            outbox
                .reclaim_expired_lease(Some("actuarius"), "worker-c", Duration::minutes(1))
                .expect("reclaim")
                .is_none(),
            "a live lease must not be reclaimed"
        );
        std::thread::sleep(std::time::Duration::from_millis(2200));
        let recovered = outbox
            .reclaim_expired_lease(Some("actuarius"), "worker-c", Duration::minutes(1))
            .expect("reclaim after expiry")
            .expect("an expired lease must be recoverable");
        assert_eq!(recovered.operation_id, op.operation_id);
        assert_eq!(recovered.lease_owner.as_deref(), Some("worker-c"));
        assert_eq!(recovered.revision, claimed.revision + 1);
    }

    #[test]
    fn claim_next_respects_lease_expiry_over_lease_state() {
        let (_dir, outbox) = store();
        let op = enqueue_active(&outbox, "entity-a", "g", "op-1");
        let claimed = expect_applied(
            outbox
                .claim_by_id(&op.operation_id, "worker-a", op.revision, Duration::minutes(1))
                .expect("claim"),
        );
        assert!(
            outbox
                .claim_next("actuarius", "worker-b", Duration::minutes(1))
                .expect("claim_next")
                .is_none(),
            "a leased operation must not be claimable by the batch claimer"
        );
        assert_eq!(claimed.lease_owner.as_deref(), Some("worker-a"));
    }

    #[test]
    fn retry_is_persisted_and_gated_on_retry_after() {
        let (dir, outbox) = store();
        let op = enqueue_active(&outbox, "entity-a", "g", "op-1");
        let claimed = expect_applied(
            outbox
                .claim_by_id(&op.operation_id, "worker-a", op.revision, Duration::minutes(1))
                .expect("claim"),
        );
        let far_future = OffsetDateTime::now_utc() + Duration::hours(1);
        let retried = expect_applied(
            outbox
                .schedule_retry(
                    &op.operation_id,
                    "worker-a",
                    claimed.revision,
                    "timeout after 5s",
                    far_future,
                )
                .expect("schedule retry"),
        );
        assert_eq!(retried.state, OutboxState::Retryable);
        assert_eq!(retried.attempt_count, 1);
        assert_eq!(retried.last_error.as_deref(), Some("timeout after 5s"));
        assert_eq!(retried.retry_after, Some(far_future));
        assert!(retried.lease_owner.is_none(), "a retry releases the lease");

        // Not due yet: neither the batch claimer nor an exact claim may take it.
        assert!(
            outbox
                .claim_next("actuarius", "worker-a", Duration::minutes(1))
                .expect("claim_next")
                .is_none(),
            "a retryable operation must not be claimed before its retry_after"
        );
        let not_due = outbox.claim_by_id(
            &op.operation_id,
            "worker-a",
            retried.revision,
            Duration::minutes(1),
        );
        assert!(
            not_due.is_err()
                && expect_invariant(&not_due.expect_err("no claim"))
                    .starts_with(OUTBOX_RETRY_NOT_DUE)
        );

        // Crash/restart persistence: reopen the file and the retryable state survives, and it is
        // still gated on the far-future retry_after after restart.
        drop(outbox);
        let reopened = OutboxStore::new(dir.path().join("storage.sqlite3"));
        reopened.ensure_schema().expect("schema");
        let reloaded = reopened.get_operation(&op.operation_id).expect("get").expect("present");
        assert_eq!(reloaded.state, OutboxState::Retryable);
        assert_eq!(reloaded.attempt_count, 1);
        assert_eq!(reloaded.last_error.as_deref(), Some("timeout after 5s"));
        assert!(
            reopened
                .claim_next("actuarius", "worker-a", Duration::minutes(1))
                .expect("claim_next")
                .is_none(),
            "the retry gate must survive a restart"
        );

        // A retryable operation whose retry_after is already in the past is claimable again, with
        // the attempt count accumulating across the cycle.
        let due = enqueue_active(&reopened, "entity-b", "h", "op-due");
        let due_claimed = expect_applied(
            reopened
                .claim_by_id(&due.operation_id, "worker-b", due.revision, Duration::minutes(1))
                .expect("claim due op"),
        );
        let due_retried = expect_applied(
            reopened
                .schedule_retry(
                    &due.operation_id,
                    "worker-b",
                    due_claimed.revision,
                    "retry now",
                    OffsetDateTime::now_utc() - Duration::seconds(1),
                )
                .expect("schedule immediate retry"),
        );
        assert_eq!(due_retried.state, OutboxState::Retryable);
        assert_eq!(due_retried.attempt_count, 1);
        let reclaimed =
            reopened.claim_next("actuarius", "worker-c", Duration::minutes(1)).expect("claim_next");
        assert_eq!(
            reclaimed
                .expect("a past retry_after must make the operation claimable again")
                .operation_id,
            due.operation_id
        );
    }

    #[test]
    fn retries_remain_retryable_beyond_the_advisory_max_and_only_fail_is_terminal() {
        let (_dir, outbox) = store();
        // max_attempts is advisory: transport/unknown failures survive arbitrary retries and
        // never exhaust into terminal failure.
        let op = outbox
            .enqueue(&NewOutboxOperation {
                created_by: "repl-worker".into(),
                idempotency_key: "op-unlimited".into(),
                mutation_kind: "drawer_added".into(),
                entity_id: "entity-a".into(),
                destination_remote: "actuarius".into(),
                ordering_key: "g".into(),
                payload: serde_json::json!({"n": 1}),
                max_attempts: 2,
            })
            .expect("enqueue");
        let active = activate(&outbox, &op);
        let mut current = expect_applied(
            outbox
                .claim_by_id(
                    &active.operation_id,
                    "worker-a",
                    active.revision,
                    Duration::minutes(1),
                )
                .expect("claim"),
        );
        // Drive the attempt count well past the advisory max (2): every schedule_retry stays
        // retryable, with the attempt count climbing and the last error retained.
        for _ in 0..4 {
            let retried = expect_applied(
                outbox
                    .schedule_retry(
                        &active.operation_id,
                        "worker-a",
                        current.revision,
                        "transient outage",
                        OffsetDateTime::now_utc() - Duration::seconds(1),
                    )
                    .expect("schedule retry"),
            );
            assert_eq!(
                retried.state,
                OutboxState::Retryable,
                "a transient failure must never exhaust into terminal failure"
            );
            assert_eq!(retried.last_error.as_deref(), Some("transient outage"));
            current = expect_applied(
                outbox
                    .claim_by_id(
                        &retried.operation_id,
                        "worker-a",
                        retried.revision,
                        Duration::minutes(1),
                    )
                    .expect("reclaim due retry"),
            );
        }
        assert_eq!(current.attempt_count, 4, "attempt_count stays observable across retries");

        // An explicit fail is terminal regardless of remaining budget.
        let explicit_fail = enqueue_active(&outbox, "entity-b", "h", "op-fail");
        let claimed = expect_applied(
            outbox
                .claim_by_id(
                    &explicit_fail.operation_id,
                    "worker-a",
                    explicit_fail.revision,
                    Duration::minutes(1),
                )
                .expect("claim"),
        );
        let failed = expect_applied(
            outbox
                .fail(
                    &explicit_fail.operation_id,
                    "worker-a",
                    claimed.revision,
                    "rejected by remote",
                )
                .expect("fail"),
        );
        assert_eq!(failed.state, OutboxState::Failed);
        assert_eq!(failed.last_error.as_deref(), Some("rejected by remote"));
        assert!(failed.lease_owner.is_none());
    }

    #[test]
    fn acknowledgement_clears_lease_and_is_idempotent() {
        let (_dir, outbox) = store();
        let op = enqueue_active(&outbox, "entity-a", "g", "op-1");
        let claimed = expect_applied(
            outbox
                .claim_by_id(&op.operation_id, "worker-a", op.revision, Duration::minutes(1))
                .expect("claim"),
        );
        let before = outbox.backlog(None).expect("backlog");
        assert_eq!((before.leased_count, before.pending_count), (1, 0));

        let acknowledged = expect_applied(
            outbox
                .acknowledge(&op.operation_id, "worker-a", claimed.revision)
                .expect("acknowledge"),
        );
        assert_eq!(acknowledged.state, OutboxState::Replicated);
        assert!(acknowledged.lease_owner.is_none());
        assert!(acknowledged.last_error.is_none());

        // Acknowledge again at the same revision is a harmless no-op returning the same op.
        let replay = expect_applied(
            outbox
                .acknowledge(&op.operation_id, "worker-a", acknowledged.revision)
                .expect("re-acknowledge"),
        );
        assert_eq!(replay, acknowledged);

        let after = outbox.backlog(None).expect("backlog");
        assert_eq!((after.leased_count, after.pending_count), (0, 0));
        assert_eq!(after.total_count, 1);
    }

    #[test]
    fn per_entity_sequences_are_monotonic_and_claim_order_follows_sequence() {
        let (_dir, outbox) = store();
        let e1 = enqueue_active(&outbox, "entity-a", "g", "e1");
        let e2 = enqueue_active(&outbox, "entity-a", "g", "e2");
        let e3 = enqueue_active(&outbox, "entity-a", "g", "e3");
        let f1 = enqueue_active(&outbox, "entity-b", "h", "f1");
        let f2 = enqueue_active(&outbox, "entity-b", "h", "f2");
        assert_eq!(
            [e1.entity_sequence, e2.entity_sequence, e3.entity_sequence],
            [1, 2, 3],
            "per-entity sequences must be monotonic"
        );
        assert_eq!([f1.entity_sequence, f2.entity_sequence], [1, 2], "sequences are per entity");

        // Draining the due heads across groups yields sequence order once each acknowledged head
        // is retired: within a group order strictly follows `sequence`, and the two groups
        // interleave by lowest sequence among due heads.
        let mut drained = Vec::new();
        while let Some(op) =
            outbox.claim_next("actuarius", "worker-a", Duration::minutes(1)).expect("claim_next")
        {
            outbox.acknowledge(&op.operation_id, "worker-a", op.revision).expect("acknowledge");
            drained.push(op.operation_id);
        }
        assert_eq!(
            drained,
            vec![
                e1.operation_id,
                e2.operation_id,
                e3.operation_id,
                f1.operation_id,
                f2.operation_id
            ],
            "draining due heads across groups interleaves in sequence order; within a group order follows sequence"
        );
    }

    #[test]
    fn leased_predecessor_blocks_later_operation_with_same_key() {
        let (_dir, outbox) = store();
        let head = enqueue_active(&outbox, "entity-a", "g", "op-head");
        let later = enqueue_active(&outbox, "entity-a", "g", "op-later");
        let claimed = expect_applied(
            outbox
                .claim_by_id(&head.operation_id, "worker-a", head.revision, Duration::minutes(1))
                .expect("claim head"),
        );
        assert_eq!(claimed.state, OutboxState::Leased);

        // The leased predecessor blocks the later operation in the same group.
        assert!(
            outbox
                .claim_next("actuarius", "worker-b", Duration::minutes(1))
                .expect("claim_next")
                .is_none(),
            "a leased predecessor must block its group"
        );
        let err = outbox
            .claim_by_id(&later.operation_id, "worker-b", later.revision, Duration::minutes(1))
            .expect_err("head-of-line claim must fail");
        assert!(expect_invariant(&err).starts_with(OUTBOX_PREDECESSOR_IN_FLIGHT), "{err}");

        // A different group's due head is unaffected.
        let other = enqueue_active(&outbox, "entity-b", "h", "op-other");
        assert_eq!(
            outbox
                .claim_next("actuarius", "worker-b", Duration::minutes(1))
                .expect("claim_next")
                .expect("other")
                .operation_id,
            other.operation_id,
            "a blocked group must not block a different, due group"
        );

        // Resolving the head unblocks the group.
        expect_applied(
            outbox.acknowledge(&head.operation_id, "worker-a", claimed.revision).expect("ack"),
        );
        assert_eq!(
            outbox
                .claim_next("actuarius", "worker-b", Duration::minutes(1))
                .expect("claim_next")
                .expect("later")
                .operation_id,
            later.operation_id,
            "acknowledging the head must unblock the group"
        );
    }

    #[test]
    fn retry_not_due_predecessor_blocks_later_operation_with_same_key() {
        let (_dir, outbox) = store();
        let head = enqueue_active(&outbox, "entity-a", "g", "op-head");
        let later = enqueue_active(&outbox, "entity-a", "g", "op-later");
        let claimed = expect_applied(
            outbox
                .claim_by_id(&head.operation_id, "worker-a", head.revision, Duration::minutes(1))
                .expect("claim head"),
        );
        let retried = expect_applied(
            outbox
                .schedule_retry(
                    &head.operation_id,
                    "worker-a",
                    claimed.revision,
                    "backoff",
                    OffsetDateTime::now_utc() + Duration::hours(1),
                )
                .expect("schedule retry"),
        );
        assert_eq!(retried.state, OutboxState::Retryable);
        assert!(retried.lease_owner.is_none());

        // The retry-not-yet-due predecessor blocks the later operation in the same group.
        assert!(
            outbox
                .claim_next("actuarius", "worker-b", Duration::minutes(1))
                .expect("claim_next")
                .is_none(),
            "a retry-not-due predecessor must block its group"
        );
        let err = outbox
            .claim_by_id(&later.operation_id, "worker-b", later.revision, Duration::minutes(1))
            .expect_err("head-of-line claim must fail");
        assert!(expect_invariant(&err).starts_with(OUTBOX_PREDECESSOR_IN_FLIGHT), "{err}");

        // The head itself is not claimable until its retry_after passes.
        let err = outbox
            .claim_by_id(&retried.operation_id, "worker-a", retried.revision, Duration::minutes(1))
            .expect_err("a not-yet-due retry must not be claimable");
        assert!(expect_invariant(&err).starts_with(OUTBOX_RETRY_NOT_DUE), "{err}");

        // A different group is unaffected.
        let other = enqueue_active(&outbox, "entity-b", "h", "op-other");
        assert_eq!(
            outbox
                .claim_next("actuarius", "worker-b", Duration::minutes(1))
                .expect("claim_next")
                .expect("other")
                .operation_id,
            other.operation_id,
            "a blocked group must not block a different, due group"
        );
    }

    #[test]
    fn blocked_key_does_not_block_a_due_operation_for_a_different_key() {
        let (_dir, outbox) = store();
        let blocked_head = enqueue_active(&outbox, "entity-a", "g", "op-head-g");
        enqueue_active(&outbox, "entity-a", "g", "op-later-g");
        expect_applied(
            outbox
                .claim_by_id(
                    &blocked_head.operation_id,
                    "worker-a",
                    blocked_head.revision,
                    Duration::minutes(1),
                )
                .expect("claim blocked head"),
        );
        // The `g` group is blocked (leased head); the `h` group stays independently deliverable.
        let due = enqueue_active(&outbox, "entity-b", "h", "op-due-h");
        let claimed = outbox
            .claim_next("actuarius", "worker-b", Duration::minutes(1))
            .expect("claim_next")
            .expect("the h group head must be claimable");
        assert_eq!(claimed.operation_id, due.operation_id);
        // The later `g` op is still blocked by the leased head.
        assert!(
            outbox
                .claim_next("actuarius", "worker-b", Duration::minutes(1))
                .expect("claim_next")
                .is_none(),
            "the blocked g group must not leak a non-head operation"
        );
    }

    #[test]
    fn more_than_100_blocked_group_heads_cannot_hide_a_due_head() {
        let (_dir, outbox) = store();
        let far_future = OffsetDateTime::now_utc() + Duration::hours(1);
        // More groups than the old candidate-scan cap, each with a not-yet-due retryable head
        // that blocks only itself.
        for i in 0..105 {
            let entity = format!("entity-blocked-{i}");
            let op_key = format!("op-blocked-{i}");
            let head = enqueue_active(&outbox, &entity, &entity, &op_key);
            let claimed = expect_applied(
                outbox
                    .claim_by_id(
                        &head.operation_id,
                        "worker-a",
                        head.revision,
                        Duration::minutes(1),
                    )
                    .expect("claim head"),
            );
            expect_applied(
                outbox
                    .schedule_retry(
                        &head.operation_id,
                        "worker-a",
                        claimed.revision,
                        "backoff",
                        far_future,
                    )
                    .expect("schedule retry"),
            );
        }
        // One due group enqueued last (highest sequence). With a truncated candidate scan the due
        // head would be hidden behind the not-due heads; the untruncated scan must return it.
        let due = enqueue_active(&outbox, "entity-due", "entity-due", "op-due");
        let claimed = outbox
            .claim_next("actuarius", "worker-b", Duration::minutes(1))
            .expect("claim_next")
            .expect("a due head beyond many not-due heads must still be returned");
        assert_eq!(claimed.operation_id, due.operation_id);
    }

    #[test]
    fn reopen_restart_persists_every_state() {
        let (dir, outbox) = store();
        let a = enqueue_active(&outbox, "entity-a", "g", "op-a");
        let b = enqueue_active(&outbox, "entity-b", "h", "op-b");
        let claimed_a = expect_applied(
            outbox
                .claim_by_id(&a.operation_id, "worker-a", a.revision, Duration::minutes(5))
                .expect("claim a"),
        );
        let claimed_b = expect_applied(
            outbox
                .claim_by_id(&b.operation_id, "worker-b", b.revision, Duration::minutes(5))
                .expect("claim b"),
        );
        let acknowledged_b = expect_applied(
            outbox
                .acknowledge(&b.operation_id, "worker-b", claimed_b.revision)
                .expect("acknowledge b"),
        );
        drop(outbox);

        let reopened = OutboxStore::new(dir.path().join("storage.sqlite3"));
        reopened.ensure_schema().expect("schema on reopen");
        let reloaded_a =
            reopened.get_operation(&a.operation_id).expect("get a").expect("a present");
        assert_eq!(reloaded_a.state, OutboxState::Leased);
        assert_eq!(reloaded_a.lease_owner.as_deref(), Some("worker-a"));
        assert!(reloaded_a.lease_expires_at.is_some());
        assert_eq!(reloaded_a.revision, claimed_a.revision);
        let reloaded_b =
            reopened.get_operation(&b.operation_id).expect("get b").expect("b present");
        assert_eq!(reloaded_b.state, OutboxState::Replicated);
        assert_eq!(reloaded_b.lease_owner, None);
        assert_eq!(reloaded_b.revision, acknowledged_b.revision);
    }

    #[test]
    fn ensure_schema_is_idempotent() {
        let dir = TempDir::new().expect("temp");
        let path = dir.path().join("storage.sqlite3");
        let outbox = OutboxStore::new(&path);
        outbox.ensure_schema().expect("first schema pass");
        outbox.ensure_schema().expect("second schema pass");
        let op = enqueue(&outbox, "entity-a", "g", "op-1");
        assert_eq!(op.entity_sequence, 1);
    }

    #[test]
    fn federation_sized_payloads_are_accepted_and_oversized_rejected() {
        let (_dir, outbox) = store();
        // Generated drawer IDs are composed from caller-controlled wing/room IDs and can exceed
        // the 256-byte limit reserved for protocol/idempotency identifiers. Entity and ordering
        // identities must preserve the full value rather than rejecting a valid drawer.
        let long_drawer_id = format!("drawer_{}", "x".repeat(512));
        let long = outbox
            .enqueue(&NewOutboxOperation {
                created_by: "repl-worker".into(),
                idempotency_key: "op-long-drawer".into(),
                mutation_kind: "drawer_added".into(),
                entity_id: long_drawer_id.clone(),
                destination_remote: "actuarius".into(),
                ordering_key: long_drawer_id.clone(),
                payload: serde_json::json!({"drawer_id": long_drawer_id}),
                max_attempts: 3,
            })
            .expect("a valid long drawer identity must be accepted");
        assert_eq!(long.entity_id.len(), 519);
        assert_eq!(long.ordering_key, long.entity_id);

        // A single drawer's content can legally approach 256 KiB and batch mutation bodies scale
        // to the 16 MiB federation limit — a payload comfortably above the old 1 MiB coordination
        // cap must be stored.
        let heavy = "x".repeat(2 * 1024 * 1024);
        let accepted = outbox
            .enqueue(&NewOutboxOperation {
                created_by: "repl-worker".into(),
                idempotency_key: "op-heavy".into(),
                mutation_kind: "drawer_added".into(),
                entity_id: "entity-big".into(),
                destination_remote: "actuarius".into(),
                ordering_key: "big".into(),
                payload: serde_json::json!({ "content": heavy }),
                max_attempts: 3,
            })
            .expect("a federation-sized payload must be accepted");
        assert!(
            serde_json::to_vec(&accepted.payload).expect("payload").len() > 1024 * 1024,
            "a payload above the old 1 MiB coordination cap must be stored"
        );
        assert!(
            outbox
                .enqueue(&NewOutboxOperation {
                    created_by: "repl-worker".into(),
                    idempotency_key: "op-oversized".into(),
                    mutation_kind: "drawer_added".into(),
                    entity_id: "entity-huge".into(),
                    destination_remote: "actuarius".into(),
                    ordering_key: "huge".into(),
                    payload: serde_json::json!({ "content": "x".repeat(MAX_PAYLOAD_BYTES + 1) }),
                    max_attempts: 3,
                })
                .is_err(),
            "a payload above the outbox bound must be rejected"
        );
        assert!(
            outbox
                .enqueue(&NewOutboxOperation {
                    created_by: "repl-worker".into(),
                    idempotency_key: "op-bad-budget".into(),
                    mutation_kind: "drawer_added".into(),
                    entity_id: "entity-a".into(),
                    destination_remote: "actuarius".into(),
                    ordering_key: "g".into(),
                    payload: serde_json::json!({}),
                    max_attempts: 0,
                })
                .is_err(),
            "max_attempts < 1 must be rejected"
        );
        assert!(
            outbox
                .enqueue(&NewOutboxOperation {
                    created_by: "repl-worker".into(),
                    idempotency_key: "op-blank-remote".into(),
                    mutation_kind: "drawer_added".into(),
                    entity_id: "entity-a".into(),
                    destination_remote: "  ".into(),
                    ordering_key: "g".into(),
                    payload: serde_json::json!({}),
                    max_attempts: 3,
                })
                .is_err(),
            "a blank destination remote must be rejected"
        );
        assert!(
            matches!(
                outbox
                    .claim_by_id("outbox_missing", "worker-a", 0, Duration::minutes(1))
                    .expect("claim call"),
                RevisionedWrite::Conflict { actual_revision: None }
            ),
            "claiming a missing operation is a typed conflict"
        );
    }

    #[test]
    fn terminal_failures_are_listable_and_observable_in_backlog() {
        let (_dir, outbox) = store();
        let op = enqueue_active(&outbox, "entity-a", "g", "op-fail");
        let claimed = expect_applied(
            outbox
                .claim_by_id(&op.operation_id, "worker-a", op.revision, Duration::minutes(1))
                .expect("claim"),
        );
        expect_applied(
            outbox
                .fail(
                    &op.operation_id,
                    "worker-a",
                    claimed.revision,
                    "remote rejected the mutation",
                )
                .expect("fail"),
        );

        let listed = outbox.list_failed(10).expect("list failed");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].operation_id, op.operation_id);
        assert_eq!(listed[0].last_error.as_deref(), Some("remote rejected the mutation"));

        let summary = outbox.backlog(None).expect("backlog");
        assert_eq!(summary.failed_count, 1);
        assert_eq!(summary.cancelled_count, 0);
        assert_eq!(summary.total_count, 1);

        // A scoped summary sees the same failure; a different remote scope sees none.
        let scoped = outbox.backlog(Some("actuarius")).expect("scoped backlog");
        assert_eq!(scoped.failed_count, 1);
        let other = outbox.backlog(Some("elsewhere")).expect("other backlog");
        assert_eq!(other.failed_count, 0);
        assert_eq!(other.total_count, 0);
    }

    #[test]
    fn error_messages_start_with_their_pinned_constants() {
        let (_dir, outbox) = store();

        // staged cannot be claimed.
        let staged = enqueue(&outbox, "entity-z", "z0", "pin-staged");
        let err = outbox
            .claim_by_id(&staged.operation_id, "worker-a", staged.revision, Duration::minutes(1))
            .expect_err("staged claim must fail");
        assert!(expect_invariant(&err).starts_with(OUTBOX_OPERATION_NOT_ACTIVATED), "{err}");

        // only a staged operation may be activated.
        let pending = enqueue_active(&outbox, "entity-z", "z1", "pin-activate");
        let err =
            outbox.activate(&pending.operation_id, pending.revision).expect_err("activate pending");
        assert!(expect_invariant(&err).starts_with(OUTBOX_ONLY_STAGED_MAY_ACTIVATE), "{err}");

        // only a staged operation may be cancelled.
        let in_flight = enqueue_active(&outbox, "entity-z", "z2", "pin-cancel");
        let claimed = expect_applied(
            outbox
                .claim_by_id(
                    &in_flight.operation_id,
                    "worker-a",
                    in_flight.revision,
                    Duration::minutes(5),
                )
                .expect("claim"),
        );
        let err =
            outbox.cancel(&in_flight.operation_id, claimed.revision).expect_err("cancel in flight");
        assert!(expect_invariant(&err).starts_with(OUTBOX_CANCELLABLE_STATE_REQUIRED), "{err}");

        // terminal operation cannot be claimed.
        let terminal = enqueue_active(&outbox, "entity-z", "z3", "pin-terminal");
        let t_claimed = expect_applied(
            outbox
                .claim_by_id(
                    &terminal.operation_id,
                    "worker-a",
                    terminal.revision,
                    Duration::minutes(1),
                )
                .expect("claim"),
        );
        let failed = expect_applied(
            outbox
                .fail(&terminal.operation_id, "worker-a", t_claimed.revision, "gone for good")
                .expect("fail"),
        );
        let err = outbox
            .claim_by_id(&terminal.operation_id, "worker-b", failed.revision, Duration::minutes(1))
            .expect_err("terminal claim must fail");
        assert!(expect_invariant(&err).starts_with(OUTBOX_OPERATION_TERMINAL), "{err}");

        // lease held by another worker.
        let held = enqueue_active(&outbox, "entity-z", "z4", "pin-held");
        let h_claimed = expect_applied(
            outbox
                .claim_by_id(&held.operation_id, "worker-a", held.revision, Duration::minutes(5))
                .expect("claim"),
        );
        let err = outbox
            .claim_by_id(&held.operation_id, "worker-b", h_claimed.revision, Duration::minutes(1))
            .expect_err("live lease claim must fail");
        assert!(expect_invariant(&err).starts_with(OUTBOX_LEASE_HELD_BY_ANOTHER_WORKER), "{err}");

        // head-of-line predecessor in flight.
        let head = enqueue_active(&outbox, "entity-z", "z5", "pin-head");
        let later = enqueue_active(&outbox, "entity-z", "z5", "pin-later");
        expect_applied(
            outbox
                .claim_by_id(&head.operation_id, "worker-a", head.revision, Duration::minutes(5))
                .expect("claim head"),
        );
        let err = outbox
            .claim_by_id(&later.operation_id, "worker-b", later.revision, Duration::minutes(1))
            .expect_err("head-of-line claim must fail");
        assert!(expect_invariant(&err).starts_with(OUTBOX_PREDECESSOR_IN_FLIGHT), "{err}");

        // retry not due.
        let not_due = enqueue_active(&outbox, "entity-z", "z6", "pin-not-due");
        let nd_claimed = expect_applied(
            outbox
                .claim_by_id(
                    &not_due.operation_id,
                    "worker-a",
                    not_due.revision,
                    Duration::minutes(5),
                )
                .expect("claim"),
        );
        let retried = expect_applied(
            outbox
                .schedule_retry(
                    &not_due.operation_id,
                    "worker-a",
                    nd_claimed.revision,
                    "backoff",
                    OffsetDateTime::now_utc() + Duration::hours(1),
                )
                .expect("schedule"),
        );
        let err = outbox
            .claim_by_id(&not_due.operation_id, "worker-a", retried.revision, Duration::minutes(1))
            .expect_err("not-yet-due retry claim must fail");
        assert!(expect_invariant(&err).starts_with(OUTBOX_RETRY_NOT_DUE), "{err}");

        // only the lease owner may transition a leased operation.
        let owned = enqueue_active(&outbox, "entity-z", "z7", "pin-owner");
        let o_claimed = expect_applied(
            outbox
                .claim_by_id(&owned.operation_id, "worker-a", owned.revision, Duration::minutes(5))
                .expect("claim"),
        );
        let err = outbox
            .acknowledge(&owned.operation_id, "worker-b", o_claimed.revision)
            .expect_err("a non-owner must not acknowledge");
        assert!(expect_invariant(&err).starts_with(OUTBOX_ONLY_LEASE_OWNER), "{err}");

        // lease has expired.
        let expired = enqueue_active(&outbox, "entity-z", "z8", "pin-expired");
        let e_claimed = expect_applied(
            outbox
                .claim_by_id(
                    &expired.operation_id,
                    "worker-a",
                    expired.revision,
                    Duration::milliseconds(1),
                )
                .expect("claim"),
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
        let err = outbox
            .acknowledge(&expired.operation_id, "worker-a", e_claimed.revision)
            .expect_err("an expired lease must not be acknowledged");
        assert!(expect_invariant(&err).starts_with(OUTBOX_LEASE_HAS_EXPIRED), "{err}");

        // not in flight.
        let parked = enqueue_active(&outbox, "entity-z", "z9", "pin-parked");
        let err = outbox
            .acknowledge(&parked.operation_id, "worker-a", parked.revision)
            .expect_err("a pending operation must not be acknowledged");
        assert!(expect_invariant(&err).starts_with(OUTBOX_OPERATION_NOT_IN_FLIGHT), "{err}");

        // lease duration out of range.
        let huge = Duration::seconds(i64::MAX);
        let oversized = enqueue_active(&outbox, "entity-z", "z10", "pin-huge");
        let err = outbox
            .claim_by_id(&oversized.operation_id, "worker-a", oversized.revision, huge)
            .expect_err("an out-of-range lease TTL must be rejected, not panic");
        assert!(expect_invariant(&err).starts_with(OUTBOX_LEASE_DURATION_OUT_OF_RANGE), "{err}");
    }
}
