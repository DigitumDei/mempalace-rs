//! Transactional, local-first coordination storage.

use std::path::{Path, PathBuf};

use mempalace_core::{SHARED_AGENT_DIARY_WING, WingId};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::types::RevisionedWrite;
use crate::{Result, StorageError};

pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_TASK_TEXT_BYTES: usize = 1024 * 1024;

/// Outcome of [`CoordinationStore::import_task`].
///
/// `replayed` distinguishes a task this call created from one returned by idempotent replay of
/// an `idempotency_key` already used by the same `created_by`. Callers that write a protocol
/// envelope or report a translated state alongside the task need that distinction: on a replay
/// the stored task is authoritative and was created from an *earlier* payload, so reporting the
/// new payload's state as though it had been applied would be a lie.
#[derive(Debug, Clone, PartialEq)]
pub struct ImportedTask {
    /// The committed task, whether newly created or replayed.
    pub task: Task,
    /// True when an existing task was returned rather than a new one created.
    pub replayed: bool,
}

/// Reserved wing name for coordination rows that existed before wings were introduced. Every
/// task and event created before this stage upgraded its schema reads back with this wing.
pub const UNSCOPED_WING: &str = "wing_unscoped";

// ─── Conflict-error message fragments ──────────────────────────────────────────
//
// Every `StorageError::Invariant` this module can raise for a claim/renew/transition/
// acknowledge conflict that is *not* a stale revision is built *from* one of the constants
// below, rather than from an inline string literal at the call site. That is deliberate, not
// decorative: the federation server (`coordination_storage_error` in
// `crates/mempalace-server/src/lib.rs`) matches on these exact constants to decide whether a
// coordination write comes back as a 409 lease/state conflict (`code: "coordination_conflict"`)
// or something else. Public and pinned here so that rewording one of these messages is a
// compile error at its construction site (rename or remove the constant) rather than a silent
// server-side reclassification of a retryable 409 into a non-retryable 400 the next time
// someone tidies up the prose. `error_messages_start_with_their_constants` (in this file's test
// module) drives every path below and asserts the produced message actually starts with its
// constant, so a call site that stops building from the constant — or a constant whose text no
// longer matches what gets produced — fails loudly here instead of silently on the server.
//
// A *stale revision* is a different shape of conflict — the caller's `expected_revision` no
// longer matches the record's current one, and the caller can recover by reloading and
// retrying with the fresh value. As of Phase 3 Stage 4 (docs/Coordination-Phase-3-Design.md)
// that case is expressed as a typed [`RevisionedWrite::Conflict`] carrying the actual revision,
// the same shape `skills.rs`/`delegation.rs` already use, instead of a message fragment a
// caller would have to parse. The constants below cover only the remaining conflicts — a live
// lease held by someone else, a terminal task, an invalid transition, wrong-owner/wrong-
// recipient — which have no revision pair to report and stay text-classified.
/// Produced by [`CoordinationStore::claim_task`] when another worker holds a live lease.
pub const LEASE_HELD_BY_ANOTHER_WORKER: &str = "task lease is held by another worker";
/// Produced by [`CoordinationStore::claim_task`] when the task is already in a terminal state.
pub const TERMINAL_TASK_CANNOT_BE_CLAIMED: &str = "terminal task cannot be claimed";
/// Produced by [`CoordinationStore::claim_task`] when the task's `expires_at` has passed; the
/// task is transitioned to [`TaskState::Expired`] in the same transaction before this is
/// returned.
pub const TASK_HAS_EXPIRED: &str = "task has expired";
/// Leading fragment of the message [`CoordinationStore::transition_task`] produces when `to` is
/// not a valid transition from the task's current state; the full message is
/// `"{INVALID_TRANSITION_PREFIX}{from} -> {to}"`.
pub const INVALID_TRANSITION_PREFIX: &str = "invalid transition ";
/// Produced by [`CoordinationStore::transition_task`] when the caller is neither the task's
/// current owner nor requesting cancellation.
pub const ONLY_OWNER_MAY_TRANSITION: &str = "only the owner may transition this task";
/// Produced by [`CoordinationStore::renew_lease`] when the caller does not hold the task's
/// current lease.
pub const ONLY_LEASE_OWNER_MAY_RENEW: &str = "only the lease owner may renew";
/// Produced by [`CoordinationStore::renew_lease`] when the task's lease has already expired.
pub const LEASE_HAS_EXPIRED: &str = "lease has expired";
/// Produced by [`CoordinationStore::acknowledge_message`] when the caller is not the message's
/// addressed recipient.
pub const ONLY_RECIPIENT_MAY_ACKNOWLEDGE: &str = "only the recipient may acknowledge";
/// Produced by [`CoordinationStore::claim_task`] and [`CoordinationStore::renew_lease`] when
/// `now + ttl` would fall outside the range `OffsetDateTime` can represent.
/// `time::OffsetDateTime`'s `Add<Duration>` panics in that case, so both call sites use
/// [`OffsetDateTime::checked_add`] and turn `None` into this error instead of aborting the
/// request.
pub const LEASE_DURATION_OUT_OF_RANGE: &str = "lease duration is out of range";
/// Trailing fragment of the message produced when a coordination call references a task or
/// message id that does not exist locally — built into `"task `{id}`{NOT_FOUND_SUFFIX}"` by the
/// internal `require_task` helper (used by [`CoordinationStore::claim_task`],
/// [`CoordinationStore::renew_lease`], [`CoordinationStore::transition_task`],
/// [`CoordinationStore::send_message`], [`CoordinationStore::put_artifact`], and
/// [`CoordinationStore::put_result`], plus the internal `task_wing` lookup), and into
/// `"message `{id}`{NOT_FOUND_SUFFIX}"` by [`CoordinationStore::acknowledge_message`]'s own
/// message lookup. This is not a claim/renew/transition/acknowledge *conflict* — it is the
/// signal `mempalace-mcp`'s `is_local_record_missing` and `mempalace-server`'s
/// `coordination_storage_error` both match on to decide "this record simply is not here, try
/// federation / answer 404" rather than some other `Invariant`. It is pinned here for the exact
/// reason the constants above are: rewording the message without also renaming this constant is
/// now a compile error at every construction site, instead of silently disabling federation
/// fallback (or misclassifying the HTTP status) for whichever path got reworded.
pub const NOT_FOUND_SUFFIX: &str = " not found";

/// Durable task lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Running,
    InputRequired,
    Completed,
    Cancelled,
    Failed,
    Expired,
}

impl TaskState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::InputRequired => "input_required",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "input_required" => Ok(Self::InputRequired),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            "expired" => Ok(Self::Expired),
            _ => Err(StorageError::Invariant(format!("unknown task state `{value}`"))),
        }
    }
    fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed | Self::Expired)
    }
}

/// Input for an idempotent task creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTask {
    pub title: String,
    pub description: String,
    pub created_by: String,
    /// Owning wing, e.g. `myproject` or `wing_myproject`. Normalised on the way in with
    /// [`WingId::normalized`], so both spellings resolve to the same wing. Required: there is
    /// no default, unlike the reserved `wing_unscoped` used for pre-upgrade rows.
    pub wing: String,
    pub idempotency_key: String,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    #[serde(default)]
    pub budget: Option<Value>,
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub expires_at: Option<OffsetDateTime>,
}

/// Authoritative task record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub task_id: String,
    pub title: String,
    pub description: String,
    pub state: TaskState,
    pub revision: i64,
    pub created_by: String,
    /// Normalised owning wing. Always present; pre-upgrade rows read back as
    /// [`UNSCOPED_WING`].
    pub wing: String,
    pub owner: Option<String>,
    pub parent_id: Option<String>,
    pub dependencies: Vec<String>,
    pub budget: Option<Value>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub lease_expires_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub expires_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// Input for an idempotent addressed message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewMessage {
    pub task_id: String,
    pub sender: String,
    pub recipient: String,
    pub kind: String,
    pub payload: Value,
    pub idempotency_key: String,
    #[serde(default = "default_envelope_version")]
    pub envelope_version: i64,
}
fn default_envelope_version() -> i64 {
    1
}

/// Append-only message record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub message_id: String,
    pub sequence: i64,
    pub task_id: String,
    pub sender: String,
    pub recipient: String,
    pub kind: String,
    pub payload: Value,
    pub envelope_version: i64,
    #[serde(with = "time::serde::rfc3339::option")]
    pub acknowledged_at: Option<OffsetDateTime>,
    pub acknowledged_by: Option<String>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Input for an immutable artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewArtifact {
    pub task_id: String,
    pub created_by: String,
    pub role: String,
    pub media_type: String,
    pub content: String,
    pub idempotency_key: String,
}

/// Immutable artifact record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Artifact {
    pub artifact_id: String,
    pub task_id: String,
    pub created_by: String,
    pub role: String,
    pub media_type: String,
    pub content: String,
    pub content_hash: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Input for an immutable, idempotent task result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTaskResult {
    pub task_id: String,
    pub created_by: String,
    pub payload: Value,
    pub idempotency_key: String,
}

/// Immutable task result record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskResult {
    pub result_id: String,
    pub task_id: String,
    pub created_by: String,
    pub payload: Value,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Opaque local ordering cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoordinationCursor(pub i64);
/// Append-only audit event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoordinationEvent {
    pub sequence: i64,
    pub event_id: String,
    pub entity_type: String,
    pub entity_id: String,
    pub task_id: Option<String>,
    /// The owning task's normalised wing, materialised at write time inside the same
    /// transaction as the mutation. Never supplied by a caller.
    pub wing: String,
    pub event_type: String,
    pub actor: String,
    pub from_state: Option<TaskState>,
    pub to_state: Option<TaskState>,
    pub revision: Option<i64>,
    pub details: Option<Value>,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
}
/// Cursor page of audit events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoordinationEventPage {
    pub events: Vec<CoordinationEvent>,
    pub next_cursor: Option<CoordinationCursor>,
}
/// Cursor page of addressed messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InboxPage {
    pub messages: Vec<Message>,
    pub next_cursor: Option<CoordinationCursor>,
}

/// Wing visibility to enforce inside [`CoordinationStore::events`] and
/// [`CoordinationStore::inbox`] *before* the SQL `LIMIT`/cursor boundary is computed.
///
/// This exists because a filter applied only after the query returns still leaks: `next_cursor`
/// computed over rows the caller cannot see (or over a `has_more` flag derived from them)
/// reveals the existence and volume of records in wings outside the caller's scope, even when
/// every row in the *response* is correctly filtered. See
/// `docs/Coordination-Phase-3-Design.md` for the incident this closes.
///
/// There is deliberately no "empty means unconstrained" case here (unlike
/// [`crate::types::DrawerFilter::wings`]): the two variants below are the only way to express
/// visibility, and an explicit empty restriction (`Federated(Some(&[]))`) always means "nothing
/// is visible", never "everything is".
#[derive(Debug, Clone, Copy)]
pub enum CoordinationVisibility<'a> {
    /// No restriction at all, including the shared diary wing
    /// ([`mempalace_core::SHARED_AGENT_DIARY_WING`]).
    ///
    /// Reserved for fully trusted, non-HTTP callers — in practice, the local MCP surface, which
    /// has no bearer-token identity to scope against. Never construct this for an
    /// HTTP-authenticated (federation) caller.
    Trusted,
    /// Every federation HTTP route, scoped or not. The shared diary wing is always excluded
    /// here, matching the existing hard override in `is_diary_wing_or_room` — no token, however
    /// unrestricted, may see it through this feed.
    ///
    /// - `None`: every other wing is visible (an unrestricted federation token).
    /// - `Some(wings)`: only the wings named in `wings` (already normalised) are visible, on top
    ///   of the diary exclusion above. `Some(&[])` is a deliberate, explicit "nothing is
    ///   visible" and is handled as such — it is never read as "no restriction".
    Federated(Option<&'a [String]>),
}

/// SQLite-backed transactional coordination repository.
#[derive(Debug, Clone)]
pub struct CoordinationStore {
    path: PathBuf,
}

impl CoordinationStore {
    /// Open coordination state in the palace's operational SQLite database.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self { path: path.as_ref().to_path_buf() }
    }

    /// Install coordination tables and indexes.
    ///
    /// Upgrade-aware: a palace created before wings existed has `coordination_tasks` and
    /// `coordination_events` tables without a `wing` column. `CREATE TABLE IF NOT EXISTS` is a
    /// no-op against those existing tables, so after it runs this checks
    /// `PRAGMA table_info` and adds the column with `ALTER TABLE ... ADD COLUMN` when it is
    /// missing. Both the fresh and the upgraded path leave `wing` as the last physical column,
    /// so the two schemas end up identical. Safe to call on every startup: adding an already-
    /// present column is skipped, not repeated.
    ///
    /// The wing index on `coordination_events` is created *after* the column backfill, not in
    /// the same batch as the rest of the schema: on an upgrade path the column does not exist
    /// yet when that batch runs, and `CREATE INDEX ... (wing, ...)` against a still-missing
    /// column would fail outright.
    pub fn ensure_schema(&self) -> Result<()> {
        let mut conn = self.connection()?;
        conn.execute_batch(r#"
CREATE TABLE IF NOT EXISTS coordination_tasks (
 task_id TEXT PRIMARY KEY, title TEXT NOT NULL, description TEXT NOT NULL, state TEXT NOT NULL,
 revision INTEGER NOT NULL, created_by TEXT NOT NULL, owner TEXT, parent_id TEXT,
 dependencies_json TEXT NOT NULL, budget_json TEXT, lease_expires_at TEXT, expires_at TEXT,
 idempotency_key TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 wing TEXT NOT NULL DEFAULT 'wing_unscoped',
 UNIQUE(created_by, idempotency_key), FOREIGN KEY(parent_id) REFERENCES coordination_tasks(task_id));
CREATE TABLE IF NOT EXISTS coordination_messages (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT UNIQUE NOT NULL, task_id TEXT NOT NULL,
 sender TEXT NOT NULL, recipient TEXT NOT NULL, kind TEXT NOT NULL, payload_json TEXT NOT NULL,
 envelope_version INTEGER NOT NULL, idempotency_key TEXT NOT NULL, acknowledged_at TEXT,
 acknowledged_by TEXT, created_at TEXT NOT NULL, UNIQUE(sender, idempotency_key),
 FOREIGN KEY(task_id) REFERENCES coordination_tasks(task_id));
CREATE INDEX IF NOT EXISTS idx_coordination_inbox ON coordination_messages(recipient, sequence);
CREATE TABLE IF NOT EXISTS coordination_artifacts (
 artifact_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, created_by TEXT NOT NULL, role TEXT NOT NULL,
 media_type TEXT NOT NULL, content TEXT NOT NULL, content_hash TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 created_at TEXT NOT NULL, UNIQUE(created_by, idempotency_key),
 FOREIGN KEY(task_id) REFERENCES coordination_tasks(task_id));
CREATE TABLE IF NOT EXISTS coordination_results (
 result_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, created_by TEXT NOT NULL,
 payload_json TEXT NOT NULL, idempotency_key TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(created_by, idempotency_key), FOREIGN KEY(task_id) REFERENCES coordination_tasks(task_id));
CREATE TABLE IF NOT EXISTS coordination_events (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL, entity_type TEXT NOT NULL,
 entity_id TEXT NOT NULL, task_id TEXT, event_type TEXT NOT NULL, actor TEXT NOT NULL,
 from_state TEXT, to_state TEXT, revision INTEGER, details_json TEXT, occurred_at TEXT NOT NULL,
 wing TEXT NOT NULL DEFAULT 'wing_unscoped');
CREATE INDEX IF NOT EXISTS idx_coordination_events_task ON coordination_events(task_id, sequence);
"#)?;
        // The check-then-act column backfill below must not run concurrently with itself across
        // processes: two MCP servers opening the same pre-Phase-3 palace at once could both
        // observe the column absent via `PRAGMA table_info` and both attempt
        // `ALTER TABLE ... ADD COLUMN`, and the loser would fail `ensure_schema` outright — a
        // startup failure at the exact moment a palace is upgraded. `BEGIN IMMEDIATE` acquires
        // the write lock up front, so a second racing connection blocks (up to
        // `PRAGMA busy_timeout`) until the first commits, then re-checks `PRAGMA table_info`
        // inside its own transaction and finds the column already present. The duplicate-column
        // error is also tolerated as a second line of defence (e.g. an external tool holding the
        // lock differently), since `add_column_if_missing` is meant to be idempotent by
        // contract, not just serialized by this transaction.
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        add_column_if_missing(&tx, "coordination_tasks", "wing", "TEXT NOT NULL DEFAULT 'wing_unscoped'")?;
        add_column_if_missing(&tx, "coordination_events", "wing", "TEXT NOT NULL DEFAULT 'wing_unscoped'")?;
        // Wing-filtered `events()` calls are a continuously polled feed, so a full scan is a
        // real cost, not a theoretical one. The column is guaranteed to exist by this point on
        // both the fresh and the upgraded path.
        tx.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_coordination_events_wing ON coordination_events(wing, sequence);",
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Create a task, or return the prior committed task for an idempotency replay.
    ///
    /// Always creates in [`TaskState::Pending`] — the only state the ordinary lifecycle
    /// (`claim_task`/`transition_task`) can start a task in. See [`Self::import_task`] for the
    /// deliberate exception: an imported task from a protocol adapter (A2A, MCP Tasks) may
    /// already have lifecycle history on the other system, and forcing it through
    /// `claim_task`/`transition_task` to reach that state would fabricate audit history (e.g. a
    /// claim by a worker that never existed).
    pub fn create_task(&self, input: &NewTask) -> Result<Task> {
        Ok(self.create_task_with_state(input, TaskState::Pending, false)?.0)
    }

    /// Create a task directly in `initial_state`, or return the prior committed task for an
    /// idempotency replay, bypassing the transition state machine entirely.
    ///
    /// This exists for protocol adapters (A2A, MCP Tasks) importing a task that already carries
    /// lifecycle history on another system: reaching, say, `Completed` via the ordinary route
    /// would require `claim_task` (asserting a worker and lease that never existed) followed by
    /// `transition_task`, which would fabricate audit history rather than record the truth ("this
    /// task arrived already in this state"). An import is a creation event, not a lifecycle
    /// transition, so it gets its own entry point rather than a hidden backdoor through
    /// `NewTask`: `NewTask` itself gains no `initial_state` field, because it is deserialized
    /// directly from `mempalace_task_create`'s MCP arguments, and a new field there would
    /// silently widen that public wire schema.
    ///
    /// The created row always has `owner = NULL` and `lease_expires_at = NULL`, even for
    /// `initial_state: Running` — an import asserts no real worker holds a lease, so none is
    /// fabricated. `claim_task`'s ownership check only rejects a claim when the task already has
    /// an owner, so an ownerless `Running` row remains claimable by any worker, and
    /// `transition_task`'s ownership check is skipped the same way, so it remains transitionable.
    /// Neither can be swept into `Expired` by an absent lease: the only automatic expiry check in
    /// this module keys off `Task::expires_at` (the lifecycle deadline), not `lease_expires_at`,
    /// and `NewTask::expires_at` is `None` unless the caller explicitly sets it.
    ///
    /// The `task_created` audit event records `initial_state` as `to_state` and carries
    /// `details: {"imported": true}`, so the trail is honest about why a freshly created task can
    /// already be non-`Pending`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Invariant`] if `initial_state` is [`TaskState::Expired`] —
    /// expiry is a lifecycle outcome this palace produces itself (see `claim_task`'s lazy expiry
    /// check), never something an importer may assert about a task it has not yet even placed
    /// under this palace's lease/expiry rules.
    pub fn import_task(
        &self,
        input: &NewTask,
        initial_state: TaskState,
    ) -> Result<ImportedTask> {
        if initial_state == TaskState::Expired {
            return Err(StorageError::Invariant(
                "TaskState::Expired is a lifecycle outcome this palace produces itself; an imported task cannot assert it as an initial state".into(),
            ));
        }
        let (task, replayed) = self.create_task_with_state(input, initial_state, true)?;
        Ok(ImportedTask { task, replayed })
    }

    fn create_task_with_state(
        &self,
        input: &NewTask,
        initial_state: TaskState,
        is_import: bool,
    ) -> Result<(Task, bool)> {
        validate_key(&input.idempotency_key)?;
        validate_actor(&input.created_by)?;
        bounded_text(&input.title, "task title")?;
        bounded_text(&input.description, "task description")?;
        if let Some(budget) = &input.budget {
            bounded_json(budget)?;
        }
        let wing = WingId::normalized(&input.wing)?.to_string();
        if wing == UNSCOPED_WING {
            return Err(StorageError::Invariant(format!(
                "`{UNSCOPED_WING}` is reserved for tasks migrated from before wings existed; new tasks must specify a real wing"
            )));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(task) = find_task_by_key(&tx, &input.created_by, &input.idempotency_key)? {
            tx.commit()?;
            return Ok((task, true));
        }
        for dependency in &input.dependencies {
            require_task(&tx, dependency)?;
        }
        if let Some(parent) = &input.parent_id {
            require_task(&tx, parent)?;
        }
        let now = OffsetDateTime::now_utc();
        let id = format!("task_{}", Uuid::new_v4().simple());
        tx.execute("INSERT INTO coordination_tasks(task_id,title,description,state,revision,created_by,owner,parent_id,dependencies_json,budget_json,lease_expires_at,expires_at,idempotency_key,created_at,updated_at,wing) VALUES (?1,?2,?3,?12,0,?4,NULL,?5,?6,?7,NULL,?8,?9,?10,?10,?11)", params![id,input.title,input.description,input.created_by,input.parent_id,serde_json::to_string(&input.dependencies)?,input.budget.as_ref().map(serde_json::to_string).transpose()?,format_time_opt(input.expires_at)?,input.idempotency_key,format_time(now)?,wing,initial_state.as_str()])?;
        let details = is_import.then(|| serde_json::json!({"imported": true}));
        append_event(
            &tx,
            "task",
            &id,
            Some(&id),
            &wing,
            "task_created",
            &input.created_by,
            None,
            Some(initial_state),
            Some(0),
            details.as_ref(),
            now,
        )?;
        let task = get_task_tx(&tx, &id)?
            .ok_or_else(|| StorageError::Invariant("created task disappeared".into()))?;
        tx.commit()?;
        Ok((task, false))
    }

    /// Retrieve a task by exact ID. `None` is an explicit authoritative miss.
    pub fn get_task(&self, id: &str) -> Result<Option<Task>> {
        get_task_conn(&self.connection()?, id)
    }

    /// List all distinct wings currently present in coordination tasks or events (excluding `wing_unscoped`).
    pub fn list_wings(&self) -> Result<Vec<String>> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT wing FROM (
                SELECT wing FROM coordination_tasks WHERE wing != ?1
                UNION
                SELECT wing FROM coordination_events WHERE wing != ?1
            ) ORDER BY wing",
        )?;
        let rows = stmt.query_map([UNSCOPED_WING], |row| row.get::<_, String>(0))?;
        let mut wings = Vec::new();
        for row in rows {
            wings.push(row?);
        }
        Ok(wings)
    }

    /// Atomically claim a task revision, reclaiming an expired lease when needed.
    ///
    /// A stale `expected_revision` returns `Ok(`[`RevisionedWrite::Conflict`]`)` carrying the
    /// task's actual current revision rather than an `Err` — the caller can reload and retry.
    /// Every other rejection (a live lease held by another worker, a terminal task, the task
    /// having just expired) is a state conflict, not a revision one, and still surfaces as an
    /// `Err(StorageError::Invariant(..))` built from one of the `pub const`s above.
    pub fn claim_task(
        &self,
        id: &str,
        worker: &str,
        expected_revision: i64,
        ttl: Duration,
    ) -> Result<RevisionedWrite<Task>> {
        validate_actor(worker)?;
        if ttl <= Duration::ZERO {
            return Err(StorageError::Invariant("lease ttl must be positive".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = require_task(&tx, id)?;
        let now = OffsetDateTime::now_utc();
        if task.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(task.revision) });
        }
        if task.state.terminal() {
            return Err(StorageError::Invariant(TERMINAL_TASK_CANNOT_BE_CLAIMED.into()));
        }
        if task.expires_at.is_some_and(|v| v <= now) {
            let next = task.revision + 1;
            tx.execute(
                "UPDATE coordination_tasks SET state='expired',revision=?2,owner=NULL,lease_expires_at=NULL,updated_at=?3 WHERE task_id=?1",
                params![id, next, format_time(now)?],
            )?;
            append_event(
                &tx,
                "task",
                id,
                Some(id),
                &task.wing,
                "task_expired",
                worker,
                Some(task.state),
                Some(TaskState::Expired),
                Some(next),
                None,
                now,
            )?;
            tx.commit()?;
            return Err(StorageError::Invariant(TASK_HAS_EXPIRED.into()));
        }
        if task.owner.as_deref().is_some_and(|owner| owner != worker)
            && task.lease_expires_at.is_some_and(|expiry| expiry > now)
        {
            return Err(StorageError::Invariant(LEASE_HELD_BY_ANOTHER_WORKER.into()));
        }
        let next = task.revision + 1;
        let expiry = now
            .checked_add(ttl)
            .ok_or_else(|| StorageError::Invariant(LEASE_DURATION_OUT_OF_RANGE.into()))?;
        let changed = tx.execute("UPDATE coordination_tasks SET state='running',revision=?2,owner=?3,lease_expires_at=?4,updated_at=?5 WHERE task_id=?1 AND revision=?6",params![id,next,worker,format_time(expiry)?,format_time(now)?,expected_revision])?;
        if changed != 1 {
            // Lost a race against another writer between the revision check above and this
            // UPDATE, inside the same `Immediate` transaction — should not happen in practice
            // (the transaction mode already serializes writers) but is handled defensively, the
            // same way `skills.rs`/`delegation.rs` treat a zero-row CAS UPDATE. The actual
            // revision is not re-read here; the caller can `get_task` for it if needed.
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        }
        append_event(
            &tx,
            "task",
            id,
            Some(id),
            &task.wing,
            if task.owner.is_some() { "task_reclaimed" } else { "task_claimed" },
            worker,
            Some(task.state),
            Some(TaskState::Running),
            Some(next),
            None,
            now,
        )?;
        let result = require_task(&tx, id)?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Renew a live lease using compare-and-swap revision semantics.
    ///
    /// See [`Self::claim_task`] for the revision-conflict-vs-state-conflict split: a stale
    /// `expected_revision` returns `Ok(`[`RevisionedWrite::Conflict`]`)`; not holding the lease,
    /// or the lease having already expired, remain `Err`.
    pub fn renew_lease(
        &self,
        id: &str,
        worker: &str,
        expected_revision: i64,
        ttl: Duration,
    ) -> Result<RevisionedWrite<Task>> {
        validate_actor(worker)?;
        if ttl <= Duration::ZERO {
            return Err(StorageError::Invariant("lease ttl must be positive".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = require_task(&tx, id)?;
        let now = OffsetDateTime::now_utc();
        if task.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(task.revision) });
        }
        if task.owner.as_deref() != Some(worker) {
            return Err(StorageError::Invariant(ONLY_LEASE_OWNER_MAY_RENEW.into()));
        }
        if task.lease_expires_at.is_none_or(|v| v <= now) {
            return Err(StorageError::Invariant(LEASE_HAS_EXPIRED.into()));
        }
        let next = task.revision + 1;
        let expiry = now
            .checked_add(ttl)
            .ok_or_else(|| StorageError::Invariant(LEASE_DURATION_OUT_OF_RANGE.into()))?;
        tx.execute("UPDATE coordination_tasks SET revision=?2,lease_expires_at=?3,updated_at=?4 WHERE task_id=?1",params![id,next,format_time(expiry)?,format_time(now)?])?;
        append_event(
            &tx,
            "task",
            id,
            Some(id),
            &task.wing,
            "lease_renewed",
            worker,
            None,
            None,
            Some(next),
            None,
            now,
        )?;
        let result = require_task(&tx, id)?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Transition lifecycle state using an expected revision.
    ///
    /// See [`Self::claim_task`] for the revision-conflict-vs-state-conflict split: a stale
    /// `expected_revision` returns `Ok(`[`RevisionedWrite::Conflict`]`)`; an invalid transition
    /// or a non-owner caller remain `Err`.
    pub fn transition_task(
        &self,
        id: &str,
        actor: &str,
        expected_revision: i64,
        to: TaskState,
        details: Option<Value>,
    ) -> Result<RevisionedWrite<Task>> {
        validate_actor(actor)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = require_task(&tx, id)?;
        let now = OffsetDateTime::now_utc();
        if task.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(task.revision) });
        }
        if !allowed_transition(task.state, to) {
            return Err(StorageError::Invariant(format!(
                "{INVALID_TRANSITION_PREFIX}{} -> {}",
                task.state.as_str(),
                to.as_str()
            )));
        }
        if task.owner.is_some()
            && task.owner.as_deref() != Some(actor)
            && to != TaskState::Cancelled
        {
            return Err(StorageError::Invariant(ONLY_OWNER_MAY_TRANSITION.into()));
        }
        let next = task.revision + 1;
        let clear = to.terminal();
        tx.execute("UPDATE coordination_tasks SET state=?2,revision=?3,owner=CASE WHEN ?4 THEN NULL ELSE owner END,lease_expires_at=CASE WHEN ?4 THEN NULL ELSE lease_expires_at END,updated_at=?5 WHERE task_id=?1",params![id,to.as_str(),next,clear,format_time(now)?])?;
        append_event(
            &tx,
            "task",
            id,
            Some(id),
            &task.wing,
            "task_transitioned",
            actor,
            Some(task.state),
            Some(to),
            Some(next),
            details.as_ref(),
            now,
        )?;
        let result = require_task(&tx, id)?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(result))
    }

    /// Send an addressed message idempotently.
    pub fn send_message(&self, input: &NewMessage) -> Result<Message> {
        validate_key(&input.idempotency_key)?;
        validate_actor(&input.sender)?;
        validate_actor(&input.recipient)?;
        bounded_json(&input.payload)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(v) = find_message_by_key(&tx, &input.sender, &input.idempotency_key)? {
            tx.commit()?;
            return Ok(v);
        }
        let task = require_task(&tx, &input.task_id)?;
        let now = OffsetDateTime::now_utc();
        let id = format!("message_{}", Uuid::new_v4().simple());
        tx.execute("INSERT INTO coordination_messages(message_id,task_id,sender,recipient,kind,payload_json,envelope_version,idempotency_key,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![id,input.task_id,input.sender,input.recipient,input.kind,serde_json::to_string(&input.payload)?,input.envelope_version,input.idempotency_key,format_time(now)?])?;
        append_event(
            &tx,
            "message",
            &id,
            Some(&input.task_id),
            &task.wing,
            "message_sent",
            &input.sender,
            None,
            None,
            None,
            Some(&serde_json::json!({"recipient":input.recipient,"kind":input.kind})),
            now,
        )?;
        let value = get_message_tx(&tx, &id)?
            .ok_or_else(|| StorageError::Invariant("created message disappeared".into()))?;
        tx.commit()?;
        Ok(value)
    }
    /// Retrieve a message by exact ID.
    pub fn get_message(&self, id: &str) -> Result<Option<Message>> {
        get_message_conn(&self.connection()?, id)
    }
    /// Read an addressed inbox after an opaque sequence cursor, optionally scoped to one wing.
    ///
    /// Messages carry no `wing` column of their own — filtering joins to the owning task's
    /// `wing` instead. `wing` is normalised the same way as task creation, so a filter of
    /// `myproject` matches messages on tasks stored as `wing_myproject`.
    pub fn inbox(
        &self,
        recipient: &str,
        cursor: Option<CoordinationCursor>,
        wing: Option<&str>,
        limit: usize,
        unacknowledged_only: bool,
        visibility: CoordinationVisibility<'_>,
    ) -> Result<InboxPage> {
        // An explicit, empty restriction means nothing is visible at all — short-circuit before
        // touching storage rather than build a `t.wing IN ()` clause (which SQLite would treat
        // differently across builds, and which is misleading to read as "restrict to nothing"
        // in the first place).
        if let CoordinationVisibility::Federated(Some(wings)) = visibility {
            if wings.is_empty() {
                return Ok(InboxPage { messages: Vec::new(), next_cursor: None });
            }
        }
        let conn = self.connection()?;
        let requested = limit.clamp(1, 500);
        let mut sql = "SELECT m.message_id,m.sequence,m.task_id,m.sender,m.recipient,m.kind,m.payload_json,m.envelope_version,m.acknowledged_at,m.acknowledged_by,m.created_at FROM coordination_messages m".to_owned();
        let mut predicates = vec!["m.recipient=?1".to_owned(), "m.sequence>?2".to_owned()];
        let mut bindings: Vec<Box<dyn rusqlite::ToSql>> =
            vec![Box::new(recipient.to_owned()), Box::new(cursor.map_or(0, |c| c.0))];
        // The task join is needed whenever an explicit wing filter or a `Federated` visibility
        // restriction requires comparing against the owning task's wing; a `Trusted` caller with
        // no wing filter never needs it.
        let needs_task_join = wing.is_some() || !matches!(visibility, CoordinationVisibility::Trusted);
        if needs_task_join {
            sql.push_str(" JOIN coordination_tasks t ON t.task_id=m.task_id");
        }
        if let Some(wing) = wing {
            let normalized = WingId::normalized(wing)?.to_string();
            bindings.push(Box::new(normalized));
            predicates.push(format!("t.wing=?{}", bindings.len()));
        }
        match visibility {
            CoordinationVisibility::Trusted => {}
            CoordinationVisibility::Federated(restrict) => {
                bindings.push(Box::new(SHARED_AGENT_DIARY_WING.to_owned()));
                predicates.push(format!("t.wing<>?{}", bindings.len()));
                if let Some(wings) = restrict {
                    let placeholders = wings
                        .iter()
                        .map(|w| {
                            bindings.push(Box::new(w.clone()));
                            format!("?{}", bindings.len())
                        })
                        .collect::<Vec<_>>();
                    predicates.push(format!("t.wing IN ({})", placeholders.join(",")));
                }
            }
        }
        if unacknowledged_only {
            predicates.push("m.acknowledged_at IS NULL".to_owned());
        }
        bindings.push(Box::new((requested + 1) as i64));
        sql.push_str(&format!(
            " WHERE {} ORDER BY m.sequence LIMIT ?{}",
            predicates.join(" AND "),
            bindings.len(),
        ));
        let mut stmt = conn.prepare(&sql)?;
        let parameters = bindings.iter().map(AsRef::as_ref).collect::<Vec<&dyn rusqlite::ToSql>>();
        let rows = stmt.query_map(parameters.as_slice(), message_row)?;
        let mut values = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        let has_more = values.len() > requested;
        values.truncate(requested);
        let next_cursor = if has_more {
            values.last().map(|message| CoordinationCursor(message.sequence))
        } else {
            None
        };
        Ok(InboxPage { messages: values, next_cursor })
    }
    /// Acknowledge a message. Replays by the same recipient are harmless.
    pub fn acknowledge_message(&self, id: &str, actor: &str) -> Result<Message> {
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let msg = get_message_tx(&tx, id)?
            .ok_or_else(|| StorageError::Invariant(format!("message `{id}`{NOT_FOUND_SUFFIX}")))?;
        if msg.recipient != actor {
            return Err(StorageError::Invariant(ONLY_RECIPIENT_MAY_ACKNOWLEDGE.into()));
        }
        if msg.acknowledged_at.is_some() {
            tx.commit()?;
            return Ok(msg);
        }
        let now = OffsetDateTime::now_utc();
        let wing = task_wing(&tx, &msg.task_id)?;
        tx.execute("UPDATE coordination_messages SET acknowledged_at=?2,acknowledged_by=?3 WHERE message_id=?1",params![id,format_time(now)?,actor])?;
        append_event(
            &tx,
            "message",
            id,
            Some(&msg.task_id),
            &wing,
            "message_acknowledged",
            actor,
            None,
            None,
            None,
            None,
            now,
        )?;
        let value = get_message_tx(&tx, id)?
            .ok_or_else(|| StorageError::Invariant("acknowledged message disappeared".into()))?;
        tx.commit()?;
        Ok(value)
    }
    /// Store an immutable artifact idempotently.
    pub fn put_artifact(&self, input: &NewArtifact) -> Result<Artifact> {
        validate_key(&input.idempotency_key)?;
        validate_actor(&input.created_by)?;
        if input.content.len() > MAX_PAYLOAD_BYTES {
            return Err(StorageError::Invariant("artifact exceeds 1 MiB".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(v) = find_artifact_by_key(&tx, &input.created_by, &input.idempotency_key)? {
            tx.commit()?;
            return Ok(v);
        }
        let task = require_task(&tx, &input.task_id)?;
        let now = OffsetDateTime::now_utc();
        let id = format!("artifact_{}", Uuid::new_v4().simple());
        let hash = blake3::hash(input.content.as_bytes()).to_hex().to_string();
        tx.execute(
            "INSERT INTO coordination_artifacts(artifact_id,task_id,created_by,role,media_type,content,content_hash,idempotency_key,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                id,
                input.task_id,
                input.created_by,
                input.role,
                input.media_type,
                input.content,
                hash,
                input.idempotency_key,
                format_time(now)?
            ],
        )?;
        append_event(
            &tx,
            "artifact",
            &id,
            Some(&input.task_id),
            &task.wing,
            "artifact_created",
            &input.created_by,
            None,
            None,
            None,
            Some(&serde_json::json!({"role":input.role,"content_hash":hash})),
            now,
        )?;
        let value = get_artifact_tx(&tx, &id)?
            .ok_or_else(|| StorageError::Invariant("created artifact disappeared".into()))?;
        tx.commit()?;
        Ok(value)
    }
    /// Retrieve an artifact by exact ID.
    pub fn get_artifact(&self, id: &str) -> Result<Option<Artifact>> {
        get_artifact_conn(&self.connection()?, id)
    }
    /// Store an immutable task result idempotently.
    pub fn put_result(&self, input: &NewTaskResult) -> Result<TaskResult> {
        validate_key(&input.idempotency_key)?;
        validate_actor(&input.created_by)?;
        bounded_json(&input.payload)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(value) = find_result_by_key(&tx, &input.created_by, &input.idempotency_key)? {
            tx.commit()?;
            return Ok(value);
        }
        let task = require_task(&tx, &input.task_id)?;
        let now = OffsetDateTime::now_utc();
        let id = format!("result_{}", Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO coordination_results(result_id,task_id,created_by,payload_json,idempotency_key,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
            params![id, input.task_id, input.created_by, serde_json::to_string(&input.payload)?, input.idempotency_key, format_time(now)?],
        )?;
        append_event(&tx, "result", &id, Some(&input.task_id), &task.wing, "result_created", &input.created_by, None, None, None, None, now)?;
        let value = get_result_tx(&tx, &id)?
            .ok_or_else(|| StorageError::Invariant("created result disappeared".into()))?;
        tx.commit()?;
        Ok(value)
    }
    /// Retrieve a task result by exact ID.
    pub fn get_result(&self, id: &str) -> Result<Option<TaskResult>> {
        get_result_conn(&self.connection()?, id)
    }
    /// Read ordered audit events after an opaque cursor, optionally scoped to one task and/or
    /// one wing. `wing` is normalised the same way as task creation, so a filter of `myproject`
    /// matches events stored as `wing_myproject`.
    pub fn events(
        &self,
        cursor: Option<CoordinationCursor>,
        task_id: Option<&str>,
        wing: Option<&str>,
        limit: usize,
        visibility: CoordinationVisibility<'_>,
    ) -> Result<CoordinationEventPage> {
        // See the identical short-circuit in `inbox`: an explicit, empty restriction means
        // nothing is visible, and must never fall through to an unconstrained query.
        if let CoordinationVisibility::Federated(Some(wings)) = visibility {
            if wings.is_empty() {
                return Ok(CoordinationEventPage { events: Vec::new(), next_cursor: None });
            }
        }
        let conn = self.connection()?;
        let requested = limit.clamp(1, 500);
        let mut predicates = vec!["sequence>?1".to_owned()];
        let mut bindings: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(cursor.map_or(0, |c| c.0))];
        if let Some(id) = task_id {
            bindings.push(Box::new(id.to_owned()));
            predicates.push(format!("task_id=?{}", bindings.len()));
        }
        if let Some(wing) = wing {
            let normalized = WingId::normalized(wing)?.to_string();
            bindings.push(Box::new(normalized));
            predicates.push(format!("wing=?{}", bindings.len()));
        }
        match visibility {
            CoordinationVisibility::Trusted => {}
            CoordinationVisibility::Federated(restrict) => {
                bindings.push(Box::new(SHARED_AGENT_DIARY_WING.to_owned()));
                predicates.push(format!("wing<>?{}", bindings.len()));
                if let Some(wings) = restrict {
                    let placeholders = wings
                        .iter()
                        .map(|w| {
                            bindings.push(Box::new(w.clone()));
                            format!("?{}", bindings.len())
                        })
                        .collect::<Vec<_>>();
                    predicates.push(format!("wing IN ({})", placeholders.join(",")));
                }
            }
        }
        bindings.push(Box::new((requested + 1) as i64));
        let sql = format!(
            "SELECT sequence,event_id,entity_type,entity_id,task_id,event_type,actor,from_state,to_state,revision,details_json,occurred_at,wing FROM coordination_events WHERE {} ORDER BY sequence LIMIT ?{}",
            predicates.join(" AND "),
            bindings.len(),
        );
        let mut stmt = conn.prepare(&sql)?;
        let parameters = bindings.iter().map(AsRef::as_ref).collect::<Vec<&dyn rusqlite::ToSql>>();
        let rows = stmt.query_map(parameters.as_slice(), event_row)?;
        let mut values = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        let has_more = values.len() > requested;
        values.truncate(requested);
        let next_cursor = if has_more {
            values.last().map(|event| CoordinationCursor(event.sequence))
        } else {
            None
        };
        Ok(CoordinationEventPage { events: values, next_cursor })
    }
    /// Retrieve an audit event by exact ID.
    pub fn get_event(&self, id: &str) -> Result<Option<CoordinationEvent>> {
        let conn = self.connection()?;
        let mut statement = conn.prepare(
            "SELECT sequence,event_id,entity_type,entity_id,task_id,event_type,actor,from_state,to_state,revision,details_json,occurred_at,wing FROM coordination_events WHERE event_id=?1",
        )?;
        statement.query_row([id], event_row).optional().map_err(Into::into)
    }

    fn connection(&self) -> Result<Connection> {
        open_palace_connection(&self.path)
    }
}

/// Add `column` to `table` with the given DDL fragment (type, nullability, default) unless it
/// already exists. `table` and `column` are always internal literals, never caller input, so
/// interpolating them into DDL text carries no injection risk; SQLite has no
/// `ADD COLUMN IF NOT EXISTS`, so the existence check is done by hand via `PRAGMA table_info`.
///
/// Callers are expected to hold a write lock (e.g. a `BEGIN IMMEDIATE` transaction) across the
/// check-and-act so two processes never race here in the first place. The duplicate-column error
/// from `ALTER TABLE` is tolerated anyway as a second line of defence: SQLite has no distinct
/// error code for it (it comes back as a generic `SQLITE_ERROR` with a message), so this matches
/// on the message text, which is a stable, documented SQLite error string for this exact
/// condition, not a moving target we control.
fn add_column_if_missing(conn: &Connection, table: &str, column: &str, ddl: &str) -> Result<()> {
    let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?
        .iter()
        .any(|name| name == column);
    if !exists {
        if let Err(err) = conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {ddl};")) {
            let message = err.to_string();
            if !message.contains("duplicate column name") {
                return Err(err.into());
            }
        }
    }
    Ok(())
}
/// Open a connection to the palace database with the pragmas every coordination-family
/// store depends on.
///
/// `journal_mode=WAL` briefly needs an exclusive lock, and SQLite does **not** retry a
/// journal-mode change through the busy handler when another connection has the database
/// open - it returns `SQLITE_BUSY` immediately no matter how long the timeout is. Two
/// processes opening the same palace at once (two MCP runtimes starting together) would
/// therefore fail outright. WAL is a persistent property of the database file, so a busy
/// here means another connection is setting it or already has: tolerate that specific
/// condition and carry on. Matched on the structured error code, never on message text.
pub(crate) fn open_palace_connection(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    // Installed through rusqlite's API rather than as a pragma in the batch below, so the
    // handler exists before the first statement that could contend runs.
    conn.busy_timeout(std::time::Duration::from_millis(5_000))?;
    conn.execute_batch("PRAGMA foreign_keys=ON;")?;
    match conn.execute_batch("PRAGMA journal_mode=WAL;") {
        Ok(()) => {}
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.code == rusqlite::ErrorCode::DatabaseBusy => {}
        Err(err) => return Err(err.into()),
    }
    Ok(conn)
}

fn validate_actor(v: &str) -> Result<()> {
    if v.trim().is_empty() {
        Err(StorageError::Invariant("actor must not be empty".into()))
    } else {
        Ok(())
    }
}
fn validate_key(v: &str) -> Result<()> {
    if v.trim().is_empty() || v.len() > 256 {
        Err(StorageError::Invariant("idempotency key must contain 1..=256 bytes".into()))
    } else {
        Ok(())
    }
}
fn bounded_json(v: &Value) -> Result<()> {
    if serde_json::to_vec(v)?.len() > MAX_PAYLOAD_BYTES {
        Err(StorageError::Invariant("payload exceeds 1 MiB".into()))
    } else {
        Ok(())
    }
}
fn bounded_text(value: &str, name: &str) -> Result<()> {
    if value.len() > MAX_TASK_TEXT_BYTES {
        Err(StorageError::Invariant(format!("{name} exceeds 1 MiB")))
    } else {
        Ok(())
    }
}
fn allowed_transition(from: TaskState, to: TaskState) -> bool {
    if from.terminal() {
        return false;
    }
    matches!(
        (from, to),
        (TaskState::Pending, TaskState::Cancelled | TaskState::Expired)
            | (
                TaskState::Running,
                TaskState::InputRequired
                    | TaskState::Completed
                    | TaskState::Cancelled
                    | TaskState::Failed
                    | TaskState::Expired
            )
            | (
                TaskState::InputRequired,
                TaskState::Pending
                    | TaskState::Running
                    | TaskState::Cancelled
                    | TaskState::Failed
                    | TaskState::Expired
            )
    )
}
fn format_time(v: OffsetDateTime) -> Result<String> {
    v.format(&Rfc3339).map_err(|e| StorageError::Invariant(e.to_string()))
}
fn format_time_opt(v: Option<OffsetDateTime>) -> Result<Option<String>> {
    v.map(format_time).transpose()
}
fn parse_time(v: String) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(&v, &Rfc3339).map_err(|e| StorageError::Invariant(e.to_string()))
}
fn parse_time_opt(v: Option<String>) -> Result<Option<OffsetDateTime>> {
    v.map(parse_time).transpose()
}
fn require_task(tx: &Transaction<'_>, id: &str) -> Result<Task> {
    get_task_tx(tx, id)?.ok_or_else(|| StorageError::Invariant(format!("task `{id}`{NOT_FOUND_SUFFIX}")))
}
/// Look up just the owning task's wing, for events whose entity isn't a `Task` itself (e.g. a
/// message acknowledgement, which only has the `Message` on hand).
fn task_wing(tx: &Transaction<'_>, task_id: &str) -> Result<String> {
    tx.query_row("SELECT wing FROM coordination_tasks WHERE task_id=?1", [task_id], |r| r.get(0))
        .optional()?
        .ok_or_else(|| StorageError::Invariant(format!("task `{task_id}`{NOT_FOUND_SUFFIX}")))
}
fn get_task_conn(conn: &Connection, id: &str) -> Result<Option<Task>> {
    let mut s=conn.prepare("SELECT task_id,title,description,state,revision,created_by,owner,parent_id,dependencies_json,budget_json,lease_expires_at,expires_at,created_at,updated_at,wing FROM coordination_tasks WHERE task_id=?1")?;
    s.query_row([id], task_row).optional().map_err(Into::into)
}
fn get_task_tx(tx: &Transaction<'_>, id: &str) -> Result<Option<Task>> {
    get_task_conn(tx, id)
}
fn find_task_by_key(tx: &Transaction<'_>, actor: &str, key: &str) -> Result<Option<Task>> {
    let id: Option<String> = tx
        .query_row(
            "SELECT task_id FROM coordination_tasks WHERE created_by=?1 AND idempotency_key=?2",
            params![actor, key],
            |r| r.get(0),
        )
        .optional()?;
    id.map(|v| require_task(tx, &v)).transpose()
}
fn task_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Task> {
    let state: String = r.get(3)?;
    let deps: String = r.get(8)?;
    let budget: Option<String> = r.get(9)?;
    let lease: Option<String> = r.get(10)?;
    let expiry: Option<String> = r.get(11)?;
    let created: String = r.get(12)?;
    let updated: String = r.get(13)?;
    let wing: String = r.get(14)?;
    Ok(Task {
        task_id: r.get(0)?,
        title: r.get(1)?,
        description: r.get(2)?,
        state: TaskState::parse(&state).map_err(sql_conv)?,
        revision: r.get(4)?,
        created_by: r.get(5)?,
        wing,
        owner: r.get(6)?,
        parent_id: r.get(7)?,
        dependencies: serde_json::from_str(&deps).map_err(sql_conv)?,
        budget: budget.map(|v| serde_json::from_str(&v)).transpose().map_err(sql_conv)?,
        lease_expires_at: parse_time_opt(lease).map_err(sql_conv)?,
        expires_at: parse_time_opt(expiry).map_err(sql_conv)?,
        created_at: parse_time(created).map_err(sql_conv)?,
        updated_at: parse_time(updated).map_err(sql_conv)?,
    })
}
fn message_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Message> {
    let payload: String = r.get(6)?;
    let ack: Option<String> = r.get(8)?;
    let created: String = r.get(10)?;
    Ok(Message {
        message_id: r.get(0)?,
        sequence: r.get(1)?,
        task_id: r.get(2)?,
        sender: r.get(3)?,
        recipient: r.get(4)?,
        kind: r.get(5)?,
        payload: serde_json::from_str(&payload).map_err(sql_conv)?,
        envelope_version: r.get(7)?,
        acknowledged_at: parse_time_opt(ack).map_err(sql_conv)?,
        acknowledged_by: r.get(9)?,
        created_at: parse_time(created).map_err(sql_conv)?,
    })
}
fn get_message_conn(conn: &Connection, id: &str) -> Result<Option<Message>> {
    let mut s=conn.prepare("SELECT message_id,sequence,task_id,sender,recipient,kind,payload_json,envelope_version,acknowledged_at,acknowledged_by,created_at FROM coordination_messages WHERE message_id=?1")?;
    s.query_row([id], message_row).optional().map_err(Into::into)
}
fn get_message_tx(tx: &Transaction<'_>, id: &str) -> Result<Option<Message>> {
    get_message_conn(tx, id)
}
fn find_message_by_key(tx: &Transaction<'_>, actor: &str, key: &str) -> Result<Option<Message>> {
    let id: Option<String> = tx
        .query_row(
            "SELECT message_id FROM coordination_messages WHERE sender=?1 AND idempotency_key=?2",
            params![actor, key],
            |r| r.get(0),
        )
        .optional()?;
    id.map(|v| {
        get_message_tx(tx, &v)?
            .ok_or_else(|| StorageError::Invariant("message key points to missing row".into()))
    })
    .transpose()
}
fn artifact_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Artifact> {
    let created: String = r.get(7)?;
    Ok(Artifact {
        artifact_id: r.get(0)?,
        task_id: r.get(1)?,
        created_by: r.get(2)?,
        role: r.get(3)?,
        media_type: r.get(4)?,
        content: r.get(5)?,
        content_hash: r.get(6)?,
        created_at: parse_time(created).map_err(sql_conv)?,
    })
}
fn get_artifact_conn(conn: &Connection, id: &str) -> Result<Option<Artifact>> {
    let mut s=conn.prepare("SELECT artifact_id,task_id,created_by,role,media_type,content,content_hash,created_at FROM coordination_artifacts WHERE artifact_id=?1")?;
    s.query_row([id], artifact_row).optional().map_err(Into::into)
}
fn get_artifact_tx(tx: &Transaction<'_>, id: &str) -> Result<Option<Artifact>> {
    get_artifact_conn(tx, id)
}
fn find_artifact_by_key(tx: &Transaction<'_>, actor: &str, key: &str) -> Result<Option<Artifact>> {
    let id:Option<String>=tx.query_row("SELECT artifact_id FROM coordination_artifacts WHERE created_by=?1 AND idempotency_key=?2",params![actor,key],|r|r.get(0)).optional()?;
    id.map(|v| {
        get_artifact_tx(tx, &v)?
            .ok_or_else(|| StorageError::Invariant("artifact key points to missing row".into()))
    })
    .transpose()
}
fn result_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<TaskResult> {
    let payload: String = r.get(3)?;
    let created: String = r.get(4)?;
    Ok(TaskResult {
        result_id: r.get(0)?,
        task_id: r.get(1)?,
        created_by: r.get(2)?,
        payload: serde_json::from_str(&payload).map_err(sql_conv)?,
        created_at: parse_time(created).map_err(sql_conv)?,
    })
}
fn get_result_conn(conn: &Connection, id: &str) -> Result<Option<TaskResult>> {
    let mut statement = conn.prepare(
        "SELECT result_id,task_id,created_by,payload_json,created_at FROM coordination_results WHERE result_id=?1",
    )?;
    statement.query_row([id], result_row).optional().map_err(Into::into)
}
fn get_result_tx(tx: &Transaction<'_>, id: &str) -> Result<Option<TaskResult>> {
    get_result_conn(tx, id)
}
fn find_result_by_key(
    tx: &Transaction<'_>,
    actor: &str,
    key: &str,
) -> Result<Option<TaskResult>> {
    let id: Option<String> = tx
        .query_row(
            "SELECT result_id FROM coordination_results WHERE created_by=?1 AND idempotency_key=?2",
            params![actor, key],
            |row| row.get(0),
        )
        .optional()?;
    id.map(|value| {
        get_result_tx(tx, &value)?
            .ok_or_else(|| StorageError::Invariant("result key points to missing row".into()))
    })
    .transpose()
}
/// Append an audit event. `wing` must be the owning task's wing, read inside the same
/// transaction as the mutation being recorded — it is never accepted from an external caller,
/// so an event can never carry a wing that disagrees with its task.
#[allow(clippy::too_many_arguments)]
fn append_event(
    tx: &Transaction<'_>,
    entity_type: &str,
    entity_id: &str,
    task_id: Option<&str>,
    wing: &str,
    event_type: &str,
    actor: &str,
    from: Option<TaskState>,
    to: Option<TaskState>,
    revision: Option<i64>,
    details: Option<&Value>,
    at: OffsetDateTime,
) -> Result<()> {
    tx.execute("INSERT INTO coordination_events(event_id,entity_type,entity_id,task_id,event_type,actor,from_state,to_state,revision,details_json,occurred_at,wing)VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",params![format!("event_{}",Uuid::new_v4().simple()),entity_type,entity_id,task_id,event_type,actor,from.map(TaskState::as_str),to.map(TaskState::as_str),revision,details.map(serde_json::to_string).transpose()?,format_time(at)?,wing])?;
    Ok(())
}
fn event_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<CoordinationEvent> {
    let from: Option<String> = r.get(7)?;
    let to: Option<String> = r.get(8)?;
    let details: Option<String> = r.get(10)?;
    let at: String = r.get(11)?;
    let wing: String = r.get(12)?;
    Ok(CoordinationEvent {
        sequence: r.get(0)?,
        event_id: r.get(1)?,
        entity_type: r.get(2)?,
        entity_id: r.get(3)?,
        task_id: r.get(4)?,
        wing,
        event_type: r.get(5)?,
        actor: r.get(6)?,
        from_state: from.map(|v| TaskState::parse(&v)).transpose().map_err(sql_conv)?,
        to_state: to.map(|v| TaskState::parse(&v)).transpose().map_err(sql_conv)?,
        revision: r.get(9)?,
        details: details.map(|v| serde_json::from_str(&v)).transpose().map_err(sql_conv)?,
        occurred_at: parse_time(at).map_err(sql_conv)?,
    })
}
fn sql_conv<E: std::error::Error + Send + Sync + 'static>(e: E) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    fn store() -> (TempDir, CoordinationStore) {
        let d = TempDir::new().expect("temp");
        let s = CoordinationStore::new(d.path().join("palace.sqlite3"));
        s.ensure_schema().expect("schema");
        (d, s)
    }
    fn task(s: &CoordinationStore) -> Task {
        s.create_task(&NewTask {
            title: "work".into(),
            description: "do it".into(),
            created_by: "manager".into(),
            wing: "wing_test".into(),
            idempotency_key: "create-1".into(),
            parent_id: None,
            dependencies: vec![],
            budget: Some(serde_json::json!({"tokens":100})),
            expires_at: None,
        })
        .expect("task")
    }
    fn task_with_wing(s: &CoordinationStore, wing: &str, key: &str) -> Task {
        s.create_task(&NewTask {
            title: "work".into(),
            description: "do it".into(),
            created_by: "manager".into(),
            wing: wing.into(),
            idempotency_key: key.into(),
            parent_id: None,
            dependencies: vec![],
            budget: None,
            expires_at: None,
        })
        .expect("task")
    }
    /// Build the pre-Phase-3 schema by hand (no `wing` column), to simulate a palace created
    /// before this stage shipped. Kept in sync with `CoordinationStore::ensure_schema`'s output
    /// minus the `wing` columns — the upgrade tests below prove the two converge.
    const LEGACY_SCHEMA_SQL: &str = r#"
CREATE TABLE coordination_tasks (
 task_id TEXT PRIMARY KEY, title TEXT NOT NULL, description TEXT NOT NULL, state TEXT NOT NULL,
 revision INTEGER NOT NULL, created_by TEXT NOT NULL, owner TEXT, parent_id TEXT,
 dependencies_json TEXT NOT NULL, budget_json TEXT, lease_expires_at TEXT, expires_at TEXT,
 idempotency_key TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 UNIQUE(created_by, idempotency_key), FOREIGN KEY(parent_id) REFERENCES coordination_tasks(task_id));
CREATE TABLE coordination_messages (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT UNIQUE NOT NULL, task_id TEXT NOT NULL,
 sender TEXT NOT NULL, recipient TEXT NOT NULL, kind TEXT NOT NULL, payload_json TEXT NOT NULL,
 envelope_version INTEGER NOT NULL, idempotency_key TEXT NOT NULL, acknowledged_at TEXT,
 acknowledged_by TEXT, created_at TEXT NOT NULL, UNIQUE(sender, idempotency_key),
 FOREIGN KEY(task_id) REFERENCES coordination_tasks(task_id));
CREATE INDEX IF NOT EXISTS idx_coordination_inbox ON coordination_messages(recipient, sequence);
CREATE TABLE coordination_artifacts (
 artifact_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, created_by TEXT NOT NULL, role TEXT NOT NULL,
 media_type TEXT NOT NULL, content TEXT NOT NULL, content_hash TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 created_at TEXT NOT NULL, UNIQUE(created_by, idempotency_key),
 FOREIGN KEY(task_id) REFERENCES coordination_tasks(task_id));
CREATE TABLE coordination_results (
 result_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, created_by TEXT NOT NULL,
 payload_json TEXT NOT NULL, idempotency_key TEXT NOT NULL, created_at TEXT NOT NULL,
 UNIQUE(created_by, idempotency_key), FOREIGN KEY(task_id) REFERENCES coordination_tasks(task_id));
CREATE TABLE coordination_events (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT UNIQUE NOT NULL, entity_type TEXT NOT NULL,
 entity_id TEXT NOT NULL, task_id TEXT, event_type TEXT NOT NULL, actor TEXT NOT NULL,
 from_state TEXT, to_state TEXT, revision INTEGER, details_json TEXT, occurred_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_coordination_events_task ON coordination_events(task_id, sequence);
"#;
    /// `(name, type, notnull, dflt_value, pk)` for every column of `table`, in physical column
    /// order — enough to prove two schemas are structurally identical.
    fn table_info(path: &Path, table: &str) -> Vec<(String, String, i64, Option<String>, i64)> {
        let conn = Connection::open(path).expect("open for pragma");
        let mut statement =
            conn.prepare(&format!("PRAGMA table_info({table})")).expect("prepare pragma");
        statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, i64>(5)?,
                ))
            })
            .expect("query pragma")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect pragma rows")
    }
    /// `(name, unique)` for every index on `table`, sorted — enough to catch an index that
    /// exists on only one of a fresh and an upgraded schema. `table_info` alone would miss
    /// that: columns can match while an index is silently absent on one side.
    fn index_list(path: &Path, table: &str) -> Vec<(String, i64)> {
        let conn = Connection::open(path).expect("open for pragma");
        let mut statement =
            conn.prepare(&format!("PRAGMA index_list({table})")).expect("prepare pragma");
        let mut rows = statement
            .query_map([], |r| Ok((r.get::<_, String>(1)?, r.get::<_, i64>(2)?)))
            .expect("query pragma")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect pragma rows");
        rows.sort();
        rows
    }
    #[test]
    fn exact_ids_and_idempotency_are_authoritative() {
        let (_d, s) = store();
        let a = task(&s);
        let b = task(&s);
        assert_eq!(a.task_id, b.task_id);
        assert_eq!(s.get_task(&a.task_id).expect("get"), Some(a));
        assert_eq!(s.get_task("task_missing").expect("miss"), None);
    }
    #[test]
    fn import_task_creates_directly_in_the_given_state_and_marks_the_event_as_imported() {
        let (_d, s) = store();
        let imported = s
            .import_task(
                &NewTask {
                    title: "already done elsewhere".into(),
                    description: "imported from another system".into(),
                    created_by: "importer".into(),
                    wing: "wing_test".into(),
                    idempotency_key: "import-completed-1".into(),
                    parent_id: None,
                    dependencies: vec![],
                    budget: None,
                    expires_at: None,
                },
                TaskState::Completed,
            )
            .expect("import");
        assert_eq!(imported.task.state, TaskState::Completed);
        assert_eq!(imported.task.revision, 0);
        assert_eq!(imported.task.owner, None);
        assert_eq!(imported.task.lease_expires_at, None);

        let events = s
            .events(None, Some(&imported.task.task_id), None, 20, CoordinationVisibility::Trusted)
            .expect("events");
        assert_eq!(events.events.len(), 1);
        let event = &events.events[0];
        assert_eq!(event.event_type, "task_created");
        assert_eq!(event.to_state, Some(TaskState::Completed));
        assert_eq!(event.details, Some(serde_json::json!({"imported": true})));
    }
    #[test]
    fn import_task_rejects_expired_as_an_initial_state() {
        let (_d, s) = store();
        let err = s
            .import_task(
                &NewTask {
                    title: "t".into(),
                    description: "d".into(),
                    created_by: "importer".into(),
                    wing: "wing_test".into(),
                    idempotency_key: "import-expired-1".into(),
                    parent_id: None,
                    dependencies: vec![],
                    budget: None,
                    expires_at: None,
                },
                TaskState::Expired,
            )
            .expect_err("Expired must never be an assertable initial state");
        assert!(matches!(err, StorageError::Invariant(_)));
    }
    #[test]
    fn import_task_is_idempotent_like_create_task() {
        let (_d, s) = store();
        let new_task = NewTask {
            title: "t".into(),
            description: "d".into(),
            created_by: "importer".into(),
            wing: "wing_test".into(),
            idempotency_key: "import-idempotent-1".into(),
            parent_id: None,
            dependencies: vec![],
            budget: None,
            expires_at: None,
        };
        let first = s.import_task(&new_task, TaskState::Running).expect("first import");
        let second = s.import_task(&new_task, TaskState::Running).expect("replay");
        assert!(!first.replayed, "the first import creates the task");
        assert!(second.replayed, "the second import replays it");
        assert_eq!(first.task.task_id, second.task.task_id);
        assert_eq!(first.task, second.task);

        // A replay whose stored state differs from the one now requested must still report the
        // *stored* task, so a caller can detect the disagreement rather than be told its new
        // state was applied.
        let third = s.import_task(&new_task, TaskState::Completed).expect("replay, other state");
        assert!(third.replayed);
        assert_eq!(third.task.state, TaskState::Running, "the stored state is authoritative");
    }

    #[test]
    fn list_wings_returns_distinct_coordination_wings_excluding_unscoped() {
        let (_d, s) = store();
        assert_eq!(s.list_wings().expect("empty wing list"), Vec::<String>::new());

        let t1 = NewTask {
            title: "t1".into(),
            description: "d1".into(),
            created_by: "creator".into(),
            wing: "wing_alpha".into(),
            idempotency_key: "task-alpha".into(),
            parent_id: None,
            dependencies: vec![],
            budget: None,
            expires_at: None,
        };
        s.create_task(&t1).expect("create t1");

        let t2 = NewTask {
            title: "t2".into(),
            description: "d2".into(),
            created_by: "creator".into(),
            wing: "wing_beta".into(),
            idempotency_key: "task-beta".into(),
            parent_id: None,
            dependencies: vec![],
            budget: None,
            expires_at: None,
        };
        s.create_task(&t2).expect("create t2");

        let wings = s.list_wings().expect("wing list");
        assert_eq!(wings, vec!["wing_alpha", "wing_beta"]);
    }
    /// Point 5 verification: a task imported directly as `Running` has no real owner or lease
    /// (an import asserts no worker ever actually claimed it), so it must remain claimable by any
    /// worker rather than becoming stuck or being swept into `Expired`. `claim_task`'s "held by
    /// another worker" check only fires when an owner already exists, so an ownerless `Running`
    /// row is unaffected by it; the only automatic expiry check in this module keys off
    /// `Task::expires_at` (which `import_task` never sets), not `lease_expires_at`.
    #[test]
    fn a_running_task_imported_with_no_owner_remains_claimable_and_is_not_swept_into_expired() {
        let (_d, s) = store();
        let imported = s
            .import_task(
                &NewTask {
                    title: "in progress elsewhere".into(),
                    description: "imported already running".into(),
                    created_by: "importer".into(),
                    wing: "wing_test".into(),
                    idempotency_key: "import-running-1".into(),
                    parent_id: None,
                    dependencies: vec![],
                    budget: None,
                    expires_at: None,
                },
                TaskState::Running,
            )
            .expect("import");
        assert_eq!(imported.task.state, TaskState::Running);
        assert_eq!(imported.task.owner, None);
        assert_eq!(imported.task.lease_expires_at, None);

        // Any worker can claim it -- there is no existing owner to conflict with.
        let claimed = applied_task(
            s.claim_task(&imported.task.task_id, "worker-a", imported.task.revision, Duration::minutes(5))
                .expect("an ownerless Running task must remain claimable"),
        );
        assert_eq!(claimed.owner.as_deref(), Some("worker-a"));
        assert_eq!(claimed.state, TaskState::Running);

        // Re-reading the freshly imported (unclaimed) task never reports it as Expired: the
        // lifecycle-expiry check keys off `expires_at`, which import_task never populates.
        let reread = s.get_task(&imported.task.task_id).expect("get").expect("still present");
        assert_ne!(reread.state, TaskState::Expired);
    }
    #[test]
    fn claim_is_cas_and_expired_lease_is_reclaimable() {
        let (_d, s) = store();
        let t = task(&s);
        let claimed =
            applied_task(s.claim_task(&t.task_id, "a", 0, Duration::milliseconds(1)).expect("claim"));
        // "b" reuses the pre-claim revision `0`, which the claim above already advanced past —
        // a stale revision, reported as a typed conflict rather than an `Err`.
        assert!(matches!(
            s.claim_task(&t.task_id, "b", 0, Duration::minutes(1)),
            Ok(RevisionedWrite::Conflict { actual_revision: Some(rev) }) if rev == claimed.revision
        ));
        std::thread::sleep(std::time::Duration::from_millis(5));
        let reclaimed = applied_task(
            s.claim_task(&t.task_id, "b", claimed.revision, Duration::minutes(1)).expect("reclaim"),
        );
        assert_eq!(reclaimed.owner.as_deref(), Some("b"));
        let events =
            s.events(None, Some(&t.task_id), None, 20, CoordinationVisibility::Trusted).expect("events");
        assert_eq!(events.events.len(), 3);
        let event = events.events.first().expect("event").clone();
        assert_eq!(s.get_event(&event.event_id).expect("exact event"), Some(event));
        assert_eq!(s.get_event("event_missing").expect("missing event"), None);
    }
    /// A lease TTL large enough that `now + ttl` overflows `OffsetDateTime`'s representable
    /// range. Regression test for the panic `time::OffsetDateTime`'s `Add<Duration>` raises on
    /// out-of-range results: both `claim_task` and `renew_lease` must turn this into a returned
    /// `StorageError::Invariant(LEASE_DURATION_OUT_OF_RANGE)`, not abort the process.
    #[test]
    fn oversized_lease_duration_is_rejected_not_a_panic() {
        let (_d, s) = store();
        let huge = Duration::seconds(i64::MAX);

        let t = task_with_wing(&s, "wing_test", "oversized-lease-claim-1");
        let err = s
            .claim_task(&t.task_id, "worker-a", t.revision, huge)
            .expect_err("an out-of-range lease TTL must be rejected, not panic");
        assert!(expect_invariant(&err).starts_with(LEASE_DURATION_OUT_OF_RANGE), "{err}");

        // Establish a live lease with a sane TTL, then try to renew it with an oversized one.
        let renewable = task_with_wing(&s, "wing_test", "oversized-lease-renew-1");
        let claimed = applied_task(
            s.claim_task(&renewable.task_id, "worker-a", renewable.revision, Duration::minutes(5))
                .expect("claim with a sane ttl"),
        );
        let err = s
            .renew_lease(&renewable.task_id, "worker-a", claimed.revision, huge)
            .expect_err("an out-of-range renewal TTL must be rejected, not panic");
        assert!(expect_invariant(&err).starts_with(LEASE_DURATION_OUT_OF_RANGE), "{err}");
    }
    /// Unwraps a `StorageError::Invariant`, panicking with the actual variant otherwise.
    fn expect_invariant(err: &StorageError) -> &str {
        match err {
            StorageError::Invariant(msg) => msg.as_str(),
            other => panic!("expected StorageError::Invariant, got {other:?}"),
        }
    }
    /// Unwraps `RevisionedWrite::Applied`, panicking with the actual conflict otherwise. Most
    /// tests below only care about the success path of a claim/renew/transition; this keeps
    /// them reading the same as before Stage 4 introduced the typed conflict shape.
    fn applied_task(write: RevisionedWrite<Task>) -> Task {
        match write {
            RevisionedWrite::Applied(task) => task,
            RevisionedWrite::Conflict { actual_revision } => {
                panic!("expected an applied write, got a conflict (actual_revision={actual_revision:?})")
            }
        }
    }
    /// Drives every conflict-shaped `Invariant` error this module can produce, plus the
    /// record-not-found case (`NOT_FOUND_SUFFIX`), and asserts the resulting message is actually
    /// built from the matching `pub const` — the same constants [`mempalace-server`'s
    /// `coordination_storage_error`] matches on to classify HTTP status, and (for
    /// `NOT_FOUND_SUFFIX` specifically) [`mempalace-mcp`'s `is_local_record_missing`] matches on
    /// to decide whether a coordination write falls back to a federated remote. This is what
    /// keeps both of those honest without either re-deriving message text of its own: a
    /// construction site that stops building its message from the constant, or a constant whose
    /// text no longer matches what actually gets produced, fails right here — loudly, at
    /// build/test time — instead of the server silently reclassifying a retryable 409 conflict as
    /// a non-retryable 400, or the MCP layer silently disabling federation fallback for that
    /// path.
    #[test]
    fn conflict_error_messages_start_with_their_pinned_constants() {
        let (_d, s) = store();

        // terminal task cannot be claimed: cancel first (Pending -> Cancelled is legal), then
        // try to claim the now-terminal task at its new, correct revision.
        let t = task(&s);
        let cancelled = applied_task(
            s.transition_task(&t.task_id, "manager", t.revision, TaskState::Cancelled, None)
                .expect("cancel"),
        );
        let err = s
            .claim_task(&t.task_id, "worker-a", cancelled.revision, Duration::minutes(1))
            .expect_err("a terminal task must not be claimable");
        assert!(expect_invariant(&err).starts_with(TERMINAL_TASK_CANNOT_BE_CLAIMED));

        // task has expired: create a task whose `expires_at` is already in the past, then claim.
        let expiring = s
            .create_task(&NewTask {
                title: "expiring".into(),
                description: "d".into(),
                created_by: "manager".into(),
                wing: "wing_test".into(),
                idempotency_key: "pin-expiring-1".into(),
                parent_id: None,
                dependencies: vec![],
                budget: None,
                expires_at: Some(OffsetDateTime::now_utc() - Duration::seconds(1)),
            })
            .expect("expiring task");
        let err = s
            .claim_task(&expiring.task_id, "worker-a", expiring.revision, Duration::minutes(1))
            .expect_err("claiming a task past its expires_at must fail");
        assert!(expect_invariant(&err).starts_with(TASK_HAS_EXPIRED));

        // task lease is held by another worker: worker-a claims, worker-b tries at the correct
        // (post-claim) revision while worker-a's lease is still live.
        let leased = task_with_wing(&s, "wing_test", "pin-leased-1");
        let claimed_leased = applied_task(
            s.claim_task(&leased.task_id, "worker-a", leased.revision, Duration::minutes(5))
                .expect("claim"),
        );
        let err = s
            .claim_task(&leased.task_id, "worker-b", claimed_leased.revision, Duration::minutes(5))
            .expect_err("a live lease held by another worker must block a second claim");
        assert!(expect_invariant(&err).starts_with(LEASE_HELD_BY_ANOTHER_WORKER));

        // invalid transition: Pending -> Completed is not an allowed edge.
        let pending = task_with_wing(&s, "wing_test", "pin-invalid-transition-1");
        let err = s
            .transition_task(&pending.task_id, "manager", pending.revision, TaskState::Completed, None)
            .expect_err("pending -> completed is not an allowed transition");
        assert!(expect_invariant(&err).starts_with(INVALID_TRANSITION_PREFIX));

        // only the owner may transition: worker-a owns the (now Running) task; worker-b tries a
        // non-cancel transition.
        let owned = task_with_wing(&s, "wing_test", "pin-owner-transition-1");
        let claimed_owned = applied_task(
            s.claim_task(&owned.task_id, "worker-a", owned.revision, Duration::minutes(5))
                .expect("claim"),
        );
        let err = s
            .transition_task(&owned.task_id, "worker-b", claimed_owned.revision, TaskState::Completed, None)
            .expect_err("a non-owner, non-cancel transition must fail");
        assert!(expect_invariant(&err).starts_with(ONLY_OWNER_MAY_TRANSITION));

        // only the lease owner may renew: worker-b tries to renew worker-a's lease.
        let renewed = task_with_wing(&s, "wing_test", "pin-renew-owner-1");
        let claimed_renewed = applied_task(
            s.claim_task(&renewed.task_id, "worker-a", renewed.revision, Duration::minutes(5))
                .expect("claim"),
        );
        let err = s
            .renew_lease(&renewed.task_id, "worker-b", claimed_renewed.revision, Duration::minutes(5))
            .expect_err("a non-owner renew must fail");
        assert!(expect_invariant(&err).starts_with(ONLY_LEASE_OWNER_MAY_RENEW));

        // lease has expired: claim with a 1ms TTL, sleep past it, then try to renew.
        let lease_expiry = task_with_wing(&s, "wing_test", "pin-lease-expiry-1");
        let claimed_lease_expiry = applied_task(
            s.claim_task(
                &lease_expiry.task_id,
                "worker-a",
                lease_expiry.revision,
                Duration::milliseconds(1),
            )
            .expect("claim"),
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
        let err = s
            .renew_lease(&lease_expiry.task_id, "worker-a", claimed_lease_expiry.revision, Duration::minutes(5))
            .expect_err("renewing an already-expired lease must fail");
        assert!(expect_invariant(&err).starts_with(LEASE_HAS_EXPIRED));

        // only the recipient may acknowledge: message addressed to "b", acknowledged by someone
        // else.
        let messaging = task_with_wing(&s, "wing_test", "pin-ack-owner-1");
        let message = s
            .send_message(&NewMessage {
                task_id: messaging.task_id,
                sender: "a".into(),
                recipient: "b".into(),
                kind: "status".into(),
                payload: serde_json::json!({}),
                idempotency_key: "pin-ack-message-1".into(),
                envelope_version: 1,
            })
            .expect("send");
        let err = s
            .acknowledge_message(&message.message_id, "not-b")
            .expect_err("acknowledging as a non-recipient must fail");
        assert!(expect_invariant(&err).starts_with(ONLY_RECIPIENT_MAY_ACKNOWLEDGE));

        // record not found: this is not a claim/renew/transition/acknowledge conflict, but it
        // shares the same "pin the constant, assert the real message is built from it" purpose —
        // `mempalace-mcp`'s `is_local_record_missing` and `mempalace-server`'s
        // `coordination_storage_error` both match on `NOT_FOUND_SUFFIX` to decide "not this
        // palace, try federation" / "answer 404", so a rewording here must fail this test the
        // same way a rewording of any conflict constant above would.
        let err = s
            .claim_task("does-not-exist", "worker-a", 1, Duration::minutes(1))
            .expect_err("claiming a nonexistent task must fail");
        assert!(expect_invariant(&err).ends_with(NOT_FOUND_SUFFIX));
        assert!(expect_invariant(&err).starts_with("task `does-not-exist`"));

        let err = s
            .acknowledge_message("does-not-exist", "worker-a")
            .expect_err("acknowledging a nonexistent message must fail");
        assert!(expect_invariant(&err).ends_with(NOT_FOUND_SUFFIX));
        assert!(expect_invariant(&err).starts_with("message `does-not-exist`"));
    }
    /// Companion to `conflict_error_messages_start_with_their_pinned_constants`: a stale
    /// `expected_revision` on claim/renew/transition is no longer a message-based `Invariant` —
    /// Phase 3 Stage 4 reconciles it onto the typed `RevisionedWrite::Conflict` shape
    /// `skills.rs`/`delegation.rs` already use, carrying the record's actual current revision so
    /// a caller can reload and retry without parsing a string. Drives all three call sites, each
    /// paired with the immediately following successful `Applied` call at the correct revision,
    /// so the two variants are never confused with each other.
    #[test]
    fn claim_renew_transition_revision_conflicts_are_typed() {
        let (_d, s) = store();

        // claim: expected_revision one ahead of the freshly created task's `0`.
        let t = task(&s);
        assert!(matches!(
            s.claim_task(&t.task_id, "worker-a", t.revision + 1, Duration::minutes(1)),
            Ok(RevisionedWrite::Conflict { actual_revision: Some(rev) }) if rev == t.revision
        ));
        let claimed = applied_task(
            s.claim_task(&t.task_id, "worker-a", t.revision, Duration::minutes(5)).expect("claim"),
        );
        assert_eq!(claimed.owner.as_deref(), Some("worker-a"));

        // renew: stale revision after the claim above advanced it.
        assert!(matches!(
            s.renew_lease(&t.task_id, "worker-a", claimed.revision + 1, Duration::minutes(5)),
            Ok(RevisionedWrite::Conflict { actual_revision: Some(rev) }) if rev == claimed.revision
        ));
        let renewed = applied_task(
            s.renew_lease(&t.task_id, "worker-a", claimed.revision, Duration::minutes(5))
                .expect("renew"),
        );

        // transition: stale revision after the renew above advanced it again.
        assert!(matches!(
            s.transition_task(
                &t.task_id,
                "worker-a",
                renewed.revision + 1,
                TaskState::Completed,
                None
            ),
            Ok(RevisionedWrite::Conflict { actual_revision: Some(rev) }) if rev == renewed.revision
        ));
        let completed = applied_task(
            s.transition_task(&t.task_id, "worker-a", renewed.revision, TaskState::Completed, None)
                .expect("transition"),
        );
        assert_eq!(completed.state, TaskState::Completed);
    }
    #[test]
    fn similar_messages_with_distinct_keys_survive_and_ack_is_authorized() {
        let (_d, s) = store();
        let t = task(&s);
        let base = NewMessage {
            task_id: t.task_id,
            sender: "a".into(),
            recipient: "b".into(),
            kind: "result".into(),
            payload: serde_json::json!({"text":"same"}),
            idempotency_key: "m1".into(),
            envelope_version: 1,
        };
        let a = s.send_message(&base).expect("a");
        let mut second = base.clone();
        second.idempotency_key = "m2".into();
        let b = s.send_message(&second).expect("b");
        assert_ne!(a.message_id, b.message_id);
        assert!(s.acknowledge_message(&a.message_id, "a").is_err());
        assert!(s.acknowledge_message(&a.message_id, "b").expect("ack").acknowledged_at.is_some());
    }
    #[test]
    fn parent_dependencies_budget_and_cancellation_are_durable() {
        let (_d, s) = store();
        let parent = task(&s);
        let child = s
            .create_task(&NewTask {
                title: "child".into(),
                description: "dependent work".into(),
                created_by: "manager".into(),
                wing: "wing_test".into(),
                idempotency_key: "child-1".into(),
                parent_id: Some(parent.task_id.clone()),
                dependencies: vec![parent.task_id.clone()],
                budget: Some(serde_json::json!({"tokens": 50})),
                expires_at: None,
            })
            .expect("child");
        assert_eq!(child.parent_id.as_deref(), Some(parent.task_id.as_str()));
        assert_eq!(child.dependencies, vec![parent.task_id]);
        assert_eq!(child.budget, Some(serde_json::json!({"tokens": 50})));
        let cancelled = applied_task(
            s.transition_task(&child.task_id, "manager", child.revision, TaskState::Cancelled, None)
                .expect("cancel"),
        );
        assert_eq!(cancelled.state, TaskState::Cancelled);
        assert!(
            s.transition_task(
                &child.task_id,
                "manager",
                cancelled.revision,
                TaskState::Running,
                None,
            )
            .is_err()
        );
    }
    #[test]
    fn concurrent_claimers_cannot_win_the_same_revision() {
        let (d, s) = store();
        let t = task(&s);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let mut joins = Vec::new();
        for worker in ["a", "b"] {
            let path = d.path().join("palace.sqlite3");
            let task_id = t.task_id.clone();
            let gate = barrier.clone();
            joins.push(std::thread::spawn(move || {
                let store = CoordinationStore::new(path);
                gate.wait();
                // Both calls now return `Ok` unconditionally — the loser gets `Ok(Conflict)`,
                // not `Err` — so only `Applied` counts as a genuine win.
                matches!(
                    store.claim_task(&task_id, worker, 0, Duration::minutes(1)),
                    Ok(RevisionedWrite::Applied(_))
                )
            }));
        }
        barrier.wait();
        let wins = joins
            .into_iter()
            .map(|join| join.join().expect("worker thread"))
            .filter(|claimed| *claimed)
            .count();
        assert_eq!(wins, 1);
    }
    #[test]
    fn lifecycle_artifacts_and_restart_recovery_are_durable() {
        let (d, s) = store();
        let t = task(&s);
        let running = applied_task(
            s.claim_task(&t.task_id, "worker", 0, Duration::minutes(1)).expect("claim"),
        );
        let waiting = applied_task(
            s.transition_task(&t.task_id, "worker", running.revision, TaskState::InputRequired, None)
                .expect("input required"),
        );
        assert_eq!(waiting.state, TaskState::InputRequired);
        assert_eq!(waiting.owner.as_deref(), Some("worker"));
        assert!(s
            .transition_task(&t.task_id, "other", waiting.revision, TaskState::Running, None)
            .is_err());
        let running_again = applied_task(
            s.transition_task(&t.task_id, "worker", waiting.revision, TaskState::Running, None)
                .expect("owner resumes task"),
        );
        assert_eq!(running_again.owner.as_deref(), Some("worker"));
        let artifact = s
            .put_artifact(&NewArtifact {
                task_id: t.task_id.clone(),
                created_by: "worker".into(),
                role: "result".into(),
                media_type: "text/plain".into(),
                content: "answer".into(),
                idempotency_key: "artifact-1".into(),
            })
            .expect("artifact");
        let result = s
            .put_result(&NewTaskResult {
                task_id: t.task_id.clone(),
                created_by: "worker".into(),
                payload: serde_json::json!({"answer": 42}),
                idempotency_key: "result-1".into(),
            })
            .expect("result");
        assert_eq!(
            s.put_result(&NewTaskResult {
                task_id: t.task_id.clone(),
                created_by: "worker".into(),
                payload: serde_json::json!({"different": "replay payload is ignored"}),
                idempotency_key: "result-1".into(),
            })
            .expect("result replay")
            .result_id,
            result.result_id
        );
        drop(s);
        let reopened = CoordinationStore::new(d.path().join("palace.sqlite3"));
        reopened.ensure_schema().expect("schema");
        assert_eq!(
            reopened.get_task(&t.task_id).expect("task").expect("found").state,
            TaskState::Running
        );
        assert_eq!(reopened.get_artifact(&artifact.artifact_id).expect("artifact"), Some(artifact));
        assert_eq!(reopened.get_result(&result.result_id).expect("result"), Some(result));
    }

    #[test]
    fn task_fields_are_bounded_and_cursors_signal_more_pages() {
        let (_d, s) = store();
        let oversized = "x".repeat(MAX_TASK_TEXT_BYTES + 1);
        assert!(s
            .create_task(&NewTask {
                title: oversized,
                description: "description".into(),
                created_by: "manager".into(),
                wing: "wing_test".into(),
                idempotency_key: "oversized-title".into(),
                parent_id: None,
                dependencies: vec![],
                budget: None,
                expires_at: None,
            })
            .is_err());

        let t = task(&s);
        for key in ["m1", "m2"] {
            s.send_message(&NewMessage {
                task_id: t.task_id.clone(),
                sender: "sender".into(),
                recipient: "receiver".into(),
                kind: "request".into(),
                payload: serde_json::json!({"key": key}),
                idempotency_key: key.into(),
                envelope_version: 1,
            })
            .expect("message");
        }
        let first = s
            .inbox("receiver", None, None, 1, false, CoordinationVisibility::Trusted)
            .expect("first inbox page");
        assert_eq!(first.messages.len(), 1);
        assert!(first.next_cursor.is_some());
        let last = s
            .inbox("receiver", first.next_cursor, None, 1, false, CoordinationVisibility::Trusted)
            .expect("last inbox page");
        assert_eq!(last.messages.len(), 1);
        assert_eq!(last.next_cursor, None);

        let first_events = s
            .events(None, Some(&t.task_id), None, 1, CoordinationVisibility::Trusted)
            .expect("first event page");
        assert_eq!(first_events.events.len(), 1);
        assert!(first_events.next_cursor.is_some());
        let mut cursor = first_events.next_cursor;
        while cursor.is_some() {
            let page = s
                .events(cursor, Some(&t.task_id), None, 500, CoordinationVisibility::Trusted)
                .expect("event page");
            cursor = page.next_cursor;
        }
    }

    #[test]
    fn ensure_schema_upgrades_a_pre_phase3_palace_and_preserves_existing_tasks() {
        let dir = TempDir::new().expect("temp");
        let path = dir.path().join("palace.sqlite3");
        {
            let conn = Connection::open(&path).expect("open");
            conn.execute_batch(LEGACY_SCHEMA_SQL).expect("legacy schema");
            conn.execute(
                "INSERT INTO coordination_tasks VALUES ('task_legacy','old title','old description','pending',0,'manager',NULL,NULL,'[]',NULL,NULL,NULL,'legacy-key','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
                [],
            )
            .expect("legacy task row");
        }

        let store = CoordinationStore::new(&path);
        store.ensure_schema().expect("upgrade schema");

        let task = store.get_task("task_legacy").expect("get").expect("legacy task survives");
        assert_eq!(task.title, "old title");
        assert_eq!(task.created_by, "manager");
        assert_eq!(task.wing, UNSCOPED_WING, "pre-existing tasks get the reserved unscoped wing");

        // The upgraded store must still be fully usable: new tasks and events on this same
        // palace get a real wing, not the reserved one.
        let fresh = task_with_wing(&store, "alpha", "post-upgrade-1");
        assert_eq!(fresh.wing, "wing_alpha");
        let events = store
            .events(None, Some(&fresh.task_id), None, 10, CoordinationVisibility::Trusted)
            .expect("events");
        assert_eq!(events.events.first().expect("event").wing, "wing_alpha");
    }

    #[test]
    fn fresh_and_upgraded_schema_end_up_identical() {
        let fresh_dir = TempDir::new().expect("temp");
        let fresh_path = fresh_dir.path().join("palace.sqlite3");
        CoordinationStore::new(&fresh_path).ensure_schema().expect("fresh schema");

        let upgraded_dir = TempDir::new().expect("temp");
        let upgraded_path = upgraded_dir.path().join("palace.sqlite3");
        {
            let conn = Connection::open(&upgraded_path).expect("open");
            conn.execute_batch(LEGACY_SCHEMA_SQL).expect("legacy schema");
        }
        CoordinationStore::new(&upgraded_path).ensure_schema().expect("upgrade schema");

        assert_eq!(
            table_info(&fresh_path, "coordination_tasks"),
            table_info(&upgraded_path, "coordination_tasks"),
            "a fresh palace and an upgraded palace must agree on coordination_tasks"
        );
        assert_eq!(
            table_info(&fresh_path, "coordination_events"),
            table_info(&upgraded_path, "coordination_events"),
            "a fresh palace and an upgraded palace must agree on coordination_events"
        );
        // Columns matching is not enough — an index present on only one path is invisible to
        // `table_info` but still a real divergence (e.g. a full scan on one side, an index seek
        // on the other).
        assert_eq!(
            index_list(&fresh_path, "coordination_tasks"),
            index_list(&upgraded_path, "coordination_tasks"),
            "a fresh palace and an upgraded palace must agree on coordination_tasks indexes"
        );
        assert_eq!(
            index_list(&fresh_path, "coordination_events"),
            index_list(&upgraded_path, "coordination_events"),
            "a fresh palace and an upgraded palace must agree on coordination_events indexes"
        );
        let event_indexes = index_list(&fresh_path, "coordination_events");
        assert!(
            event_indexes.iter().any(|(name, _)| name == "idx_coordination_events_wing"),
            "the wing index must actually be present, not merely agreed-upon: {event_indexes:?}"
        );
    }

    #[test]
    fn concurrent_ensure_schema_on_a_pre_phase3_palace_does_not_fail_the_backfill() {
        // Simulates two MCP processes opening the same pre-Phase-3 palace at the same instant:
        // both would see `wing` absent from `PRAGMA table_info` and race to `ALTER TABLE ... ADD
        // COLUMN`. Before the fix, the loser's `ensure_schema` (and therefore `McpRuntime::new`)
        // failed outright.
        let dir = TempDir::new().expect("temp");
        let path = dir.path().join("palace.sqlite3");
        {
            let conn = Connection::open(&path).expect("open");
            conn.execute_batch(LEGACY_SCHEMA_SQL).expect("legacy schema");
        }
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let mut joins = Vec::new();
        for _ in 0..2 {
            let path = path.clone();
            let gate = barrier.clone();
            joins.push(std::thread::spawn(move || {
                let store = CoordinationStore::new(path);
                gate.wait();
                store.ensure_schema()
            }));
        }
        for join in joins {
            join.join().expect("racing thread").expect("ensure_schema must not fail under a race");
        }

        let wing_columns = table_info(&path, "coordination_tasks")
            .into_iter()
            .filter(|(name, ..)| name == "wing")
            .count();
        assert_eq!(wing_columns, 1, "racing backfills must not duplicate the column");

        let store = CoordinationStore::new(&path);
        let t = task_with_wing(&store, "race", "concurrent-ensure-schema-1");
        assert_eq!(t.wing, "wing_race");
    }

    #[test]
    fn ensure_schema_is_idempotent() {
        let dir = TempDir::new().expect("temp");
        let path = dir.path().join("palace.sqlite3");
        let store = CoordinationStore::new(&path);
        store.ensure_schema().expect("first call");
        store.ensure_schema().expect("second call");
        store.ensure_schema().expect("third call");

        let wing_columns = table_info(&path, "coordination_tasks")
            .into_iter()
            .filter(|(name, ..)| name == "wing")
            .count();
        assert_eq!(wing_columns, 1, "repeated ensure_schema calls must not duplicate the column");

        // The store must still work after repeated calls: McpRuntime::new calls this on every
        // startup, so this is the realistic shape of the guarantee.
        let t = task_with_wing(&store, "repeat", "idempotence-1");
        assert_eq!(store.get_task(&t.task_id).expect("get").expect("found").wing, "wing_repeat");
    }

    #[test]
    fn task_wing_is_normalised_on_create() {
        let (_d, s) = store();
        let t = task_with_wing(&s, "myproject", "wing-normalise-1");
        assert_eq!(t.wing, "wing_myproject");
        let fetched = s.get_task(&t.task_id).expect("get").expect("found");
        assert_eq!(fetched.wing, "wing_myproject");
    }

    #[test]
    fn create_task_rejects_the_reserved_unscoped_wing_in_either_spelling() {
        let (_d, s) = store();
        s.create_task(&NewTask {
            title: "work".into(),
            description: "do it".into(),
            created_by: "manager".into(),
            wing: UNSCOPED_WING.into(),
            idempotency_key: "reject-prefixed".into(),
            parent_id: None,
            dependencies: vec![],
            budget: None,
            expires_at: None,
        })
        .expect_err("prefixed wing_unscoped must be rejected");
        s.create_task(&NewTask {
            title: "work".into(),
            description: "do it".into(),
            created_by: "manager".into(),
            wing: "unscoped".into(),
            idempotency_key: "reject-unprefixed".into(),
            parent_id: None,
            dependencies: vec![],
            budget: None,
            expires_at: None,
        })
        .expect_err("unprefixed unscoped must be rejected after normalisation");
    }

    #[test]
    fn events_inherit_the_owning_tasks_wing_and_callers_cannot_override_it() {
        let (_d, s) = store();
        let t = task_with_wing(&s, "alpha", "wing-events-1");
        s.send_message(&NewMessage {
            task_id: t.task_id.clone(),
            sender: "manager".into(),
            recipient: "worker".into(),
            kind: "handoff".into(),
            payload: serde_json::json!({}),
            idempotency_key: "wing-events-msg".into(),
            envelope_version: 1,
        })
        .expect("message");
        let claimed = applied_task(
            s.claim_task(&t.task_id, "worker", t.revision, Duration::minutes(1)).expect("claim"),
        );
        s.put_artifact(&NewArtifact {
            task_id: t.task_id.clone(),
            created_by: "worker".into(),
            role: "note".into(),
            media_type: "text/plain".into(),
            content: "hi".into(),
            idempotency_key: "wing-events-artifact".into(),
        })
        .expect("artifact");
        s.put_result(&NewTaskResult {
            task_id: t.task_id.clone(),
            created_by: "worker".into(),
            payload: serde_json::json!({"ok": true}),
            idempotency_key: "wing-events-result".into(),
        })
        .expect("result");
        s.transition_task(&t.task_id, "worker", claimed.revision, TaskState::Completed, None)
            .expect("transition");

        // Note that none of NewMessage, NewArtifact, or NewTaskResult has a `wing` field at
        // all — there is no code path through which a caller could supply one.
        let page = s
            .events(None, Some(&t.task_id), None, 50, CoordinationVisibility::Trusted)
            .expect("events");
        assert_eq!(
            page.events.len(),
            6,
            "task_created, message_sent, task_claimed, artifact_created, result_created, task_transitioned"
        );
        assert!(
            page.events.iter().all(|event| event.wing == "wing_alpha"),
            "every event must inherit the owning task's normalised wing"
        );
    }

    #[test]
    fn wing_filter_scopes_events_and_inbox_with_normalisation() {
        let (_d, s) = store();
        let alpha = task_with_wing(&s, "alpha", "wf-alpha");
        let beta = task_with_wing(&s, "beta", "wf-beta");

        s.send_message(&NewMessage {
            task_id: alpha.task_id.clone(),
            sender: "manager".into(),
            recipient: "worker".into(),
            kind: "handoff".into(),
            payload: serde_json::json!({}),
            idempotency_key: "wf-msg-alpha".into(),
            envelope_version: 1,
        })
        .expect("alpha message");
        s.send_message(&NewMessage {
            task_id: beta.task_id.clone(),
            sender: "manager".into(),
            recipient: "worker".into(),
            kind: "handoff".into(),
            payload: serde_json::json!({}),
            idempotency_key: "wf-msg-beta".into(),
            envelope_version: 1,
        })
        .expect("beta message");

        // The filter uses the unprefixed spelling; it must still match the normalised,
        // `wing_`-prefixed value that was actually stored.
        let alpha_events = s
            .events(None, None, Some("alpha"), 100, CoordinationVisibility::Trusted)
            .expect("alpha events");
        assert!(!alpha_events.events.is_empty());
        assert!(
            alpha_events.events.iter().all(|e| e.task_id.as_deref() == Some(alpha.task_id.as_str())),
            "an alpha wing filter must not leak beta's events"
        );

        let alpha_inbox = s
            .inbox("worker", None, Some("alpha"), 100, false, CoordinationVisibility::Trusted)
            .expect("alpha inbox");
        assert_eq!(alpha_inbox.messages.len(), 1);
        assert_eq!(alpha_inbox.messages[0].task_id, alpha.task_id);

        // The already-prefixed spelling must match too, on both sides of the earlier asymmetry.
        let beta_inbox = s
            .inbox("worker", None, Some("wing_beta"), 100, false, CoordinationVisibility::Trusted)
            .expect("beta inbox");
        assert_eq!(beta_inbox.messages.len(), 1);
        assert_eq!(beta_inbox.messages[0].task_id, beta.task_id);

        // No filter at all still returns both.
        let unfiltered = s
            .inbox("worker", None, None, 100, false, CoordinationVisibility::Trusted)
            .expect("unfiltered inbox");
        assert_eq!(unfiltered.messages.len(), 2);
    }

    /// `Federated(Some(&[]))` is an explicit "nothing is visible" and must short-circuit to an
    /// empty page for both feeds — never silently read as "unconstrained" the way an empty
    /// `DrawerFilter::wings` is. Regression test for the shape `CoordinationVisibility` was
    /// deliberately designed to make impossible to get wrong.
    #[test]
    fn federated_visibility_with_empty_wing_list_sees_nothing() {
        let (_d, s) = store();
        let t = task_with_wing(&s, "alpha", "empty-visibility-task");
        s.send_message(&NewMessage {
            task_id: t.task_id.clone(),
            sender: "manager".into(),
            recipient: "worker".into(),
            kind: "handoff".into(),
            payload: serde_json::json!({}),
            idempotency_key: "empty-visibility-msg".into(),
            envelope_version: 1,
        })
        .expect("message");

        let empty: Vec<String> = Vec::new();
        let events = s
            .events(None, None, None, 100, CoordinationVisibility::Federated(Some(&empty)))
            .expect("events");
        assert!(events.events.is_empty());
        assert_eq!(events.next_cursor, None);

        let inbox = s
            .inbox(
                "worker",
                None,
                None,
                100,
                false,
                CoordinationVisibility::Federated(Some(&empty)),
            )
            .expect("inbox");
        assert!(inbox.messages.is_empty());
        assert_eq!(inbox.next_cursor, None);
    }
}
