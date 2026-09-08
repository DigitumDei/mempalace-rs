//! Durable mutation receipts for idempotent federation mutations (issue #127, receiving side).
//!
//! A replicated client can retry a mutation after an `UnknownOutcome` (the response was lost
//! after the server applied it) or resend after a crash. Without a shared identity, such a retry
//! would apply the mutation twice — a duplicate drawer, a duplicated KG fact, or a rejected
//! duplicate-content add. This module supplies the receiving-side substrate: a SQLite-backed
//! receipt row, keyed by the client's stable `operation_id`, that records the request and the
//! completed response so a replayed mutation can be answered from durable state instead of being
//! re-applied.
//!
//! # Receipt lifecycle
//!
//! ```text
//! begin_receipt (pending) --complete_receipt--> completed
//! ```
//!
//! [`MutationReceiptStore::begin_receipt`] is the only entry point. Given a
//! `(operation_id, operation_kind, request_hash, target_id)` it atomically:
//!
//! - creates a fresh `pending` receipt when the `operation_id` is unknown, telling the caller to
//!   perform the mutation and confirm with [`MutationReceiptStore::complete_receipt`]
//!   ([`ReceiptOutcome::Fresh`]);
//! - replays the stored response — without any side effects — when the **same** request already
//!   completed ([`ReceiptOutcome::Replay`]);
//! - returns the pending receipt when a prior attempt started but never completed — a crash
//!   between `begin_receipt` and `complete_receipt` — so the caller can inspect the target's
//!   stable state and converge ([`ReceiptOutcome::Recover`]);
//! - reports a conflict when the `operation_id` is reused with a **different** request hash
//!   ([`ReceiptOutcome::Conflict`]).
//!
//! The request hash is caller-computed over the mutation-affecting request fields, so no two
//! distinct requests can collide on the same `operation_id` and silently share a receipt.
//!
//! # Crash recovery
//!
//! A `pending` receipt is the only state left behind by a crash mid-mutation. Because
//! [`ReceiptOutcome::Recover`] returns the receipt itself, the handler can inspect its `target_id`
//! against the stable target state (e.g. does the drawer/fact exist?) and either complete the
//! receipt with the already-applied effect's response or apply the mutation and then complete it.
//! [`MutationReceiptStore::pending_receipts`] additionally exposes every pending receipt so a
//! process can reconcile at startup, although per-request recovery via [`ReceiptOutcome::Recover`]
//! is the primary path.

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::coordination::add_column_if_missing;
use crate::{Result, StorageError};

const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Operation kind for a drawer-add mutation.
pub const RECEIPT_KIND_DRAWER_ADD: &str = "drawer_add";
/// Operation kind for a drawer-delete mutation.
pub const RECEIPT_KIND_DRAWER_DELETE: &str = "drawer_delete";
/// Operation kind for a knowledge-graph fact-add mutation.
pub const RECEIPT_KIND_KG_ADD: &str = "kg_add";
/// Operation kind for a knowledge-graph fact-invalidate mutation.
pub const RECEIPT_KIND_KG_INVALIDATE: &str = "kg_invalidate";

/// Lifecycle state of a mutation receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptState {
    /// A mutation may still be in flight: `begin_receipt` succeeded but `complete_receipt` has
    /// not. A crashed attempt leaves a receipt here; the handler must inspect stable target state.
    Pending,
    /// The mutation completed and the response is durable; an identical request replays it.
    Completed,
}

/// A single durable mutation receipt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MutationReceipt {
    /// The client-supplied stable operation identity this receipt is keyed on.
    pub operation_id: String,
    /// Which kind of mutation this receipt covers (see `RECEIPT_KIND_*`).
    pub operation_kind: String,
    /// Caller-computed hash of the mutation-affecting request fields.
    pub request_hash: String,
    /// The stable target identity the mutation converges on (drawer id, or the canonical KG
    /// triple). Recovery inspects this against the target's state.
    pub target_id: String,
    /// Lifecycle state.
    pub status: ReceiptState,
    /// The stored successful response, present once `status` is [`ReceiptState::Completed`].
    pub response: Option<Value>,
    /// Caller-supplied durable metadata about the mutation's effect (e.g. a delete's
    /// `{"wing", "room"}`), captured **before** the effect runs so a crash between the effect
    /// and its completion can be converged from. Populated via
    /// [`MutationReceiptStore::set_receipt_details`].
    pub details: Option<Value>,
    /// When the receipt was first created.
    pub created_at: OffsetDateTime,
    /// When the mutation confirmed completion, if it has.
    pub completed_at: Option<OffsetDateTime>,
}

/// Outcome of [`MutationReceiptStore::begin_receipt`].
#[derive(Debug, Clone, PartialEq)]
pub enum ReceiptOutcome {
    /// A pending receipt was freshly created; the caller must apply the mutation and then call
    /// [`MutationReceiptStore::complete_receipt`] before responding.
    Fresh(MutationReceipt),
    /// The identical request already completed; the caller must return the stored response and
    /// perform **no** side effects.
    Replay(MutationReceipt),
    /// A prior attempt started but never completed (crash or concurrent duplicate). The caller
    /// must inspect the target's stable state, converge (apply the mutation or confirm the effect
    /// is already present), then complete the receipt.
    Recover(MutationReceipt),
    /// The `operation_id` was reused with a different request; the receiver must reject with a
    /// conflict so the two requests cannot silently share one response.
    Conflict {
        /// The reused operation identity, so the receiver can surface it in the conflict error.
        operation_id: String,
    },
}

/// Input for [`MutationReceiptStore::begin_receipt`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewReceipt {
    /// Stable client operation identity; must be non-empty and at most 256 bytes.
    pub operation_id: String,
    /// One of the `RECEIPT_KIND_*` constants.
    pub operation_kind: String,
    /// Hash of the mutation-affecting request fields.
    pub request_hash: String,
    /// Stable target identity the mutation converges on.
    pub target_id: String,
}

/// SQLite-backed durable mutation receipt repository.
///
/// Opens the palace's operational SQLite database (`storage.sqlite3`), the same file the outbox,
/// coordination, and operational stores use. Every method opens its own short-lived connection and
/// commits each state change as its own transaction, so a crash never leaves a partially-written
/// receipt.
#[derive(Debug, Clone)]
pub struct MutationReceiptStore {
    path: PathBuf,
}

impl MutationReceiptStore {
    /// Open the receipt store in the palace's operational SQLite database.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self { path: path.as_ref().to_path_buf() }
    }

    /// Install the receipt table. Idempotent and safe to call on every startup.
    pub fn ensure_schema(&self) -> Result<()> {
        let mut conn = self.connection()?;
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS mutation_receipts (
    operation_id   TEXT PRIMARY KEY,
    operation_kind TEXT NOT NULL,
    request_hash   TEXT NOT NULL,
    target_id      TEXT NOT NULL,
    status         TEXT NOT NULL CHECK(status IN ('pending', 'completed')),
    response_json  TEXT,
    details_json   TEXT,
    created_at     TEXT NOT NULL,
    completed_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_mutation_receipts_status
    ON mutation_receipts(status, created_at);
"#,
        )?;
        // Upgrade path: a palace created before finding 6 has `mutation_receipts` without
        // `details_json`. `CREATE TABLE IF NOT EXISTS` is a no-op against that table, so after
        // it runs this checks `PRAGMA table_info` and adds the column when missing. `BEGIN
        // IMMEDIATE` serialises the check-then-act across processes (see the identical pattern
        // in `CoordinationStore::ensure_schema`).
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        add_column_if_missing(&tx, "mutation_receipts", "details_json", "TEXT")?;
        tx.commit()?;
        Ok(())
    }

    /// Introduce a mutation intent under an `operation_id`, or classify the replay.
    ///
    /// See the module docs and [`ReceiptOutcome`] for the four outcomes. The comparison against
    /// the prior request hash is what turns "same operation, different request" into a conflict:
    /// only a byte-identical request is allowed to reuse a completed receipt.
    pub fn begin_receipt(&self, input: &NewReceipt) -> Result<ReceiptOutcome> {
        bounded_identifier(&input.operation_id, "operation_id")?;
        bounded_identifier(&input.operation_kind, "operation_kind")?;
        bounded_identifier(&input.request_hash, "request_hash")?;
        // Target ids are validated by the owning domain (e.g. `DrawerId`), which
        // deliberately has no artificial byte ceiling.  Keep the receipt's
        // operation metadata bounded, but preserve the full target so a retry
        // can address the exact same entity.
        nonempty_identifier(&input.target_id, "target_id")?;

        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = receipt_by_id(&tx, &input.operation_id)? {
            if existing.request_hash != input.request_hash {
                tx.commit()?;
                return Ok(ReceiptOutcome::Conflict { operation_id: input.operation_id.clone() });
            }
            tx.commit()?;
            return match existing.status {
                ReceiptState::Completed => Ok(ReceiptOutcome::Replay(existing)),
                ReceiptState::Pending => Ok(ReceiptOutcome::Recover(existing)),
            };
        }

        let now = OffsetDateTime::now_utc();
        let created = format_time(now)?;
        tx.execute(
            "INSERT INTO mutation_receipts(operation_id, operation_kind, request_hash, target_id,\
             status, response_json, details_json, created_at, completed_at)\
             VALUES(?1, ?2, ?3, ?4, 'pending', NULL, NULL, ?5, NULL)",
            params![
                input.operation_id,
                input.operation_kind,
                input.request_hash,
                input.target_id,
                created,
            ],
        )?;
        let receipt = receipt_by_id(&tx, &input.operation_id)?
            .ok_or_else(|| StorageError::Invariant("inserted receipt disappeared".into()))?;
        tx.commit()?;
        Ok(ReceiptOutcome::Fresh(receipt))
    }

    /// Confirm a pending receipt as completed and persist the response to replay.
    ///
    /// Idempotent: completing an already-completed receipt is a no-op that keeps the original
    /// response, so two concurrent handlers racing to finish the same operation cannot overwrite
    /// one another's committed response. Completing an unknown `operation_id` is an invariant
    /// error — a receipt can only be completed after [`Self::begin_receipt`] created it.
    pub fn complete_receipt(&self, operation_id: &str, response: &Value) -> Result<()> {
        bounded_identifier(operation_id, "operation_id")?;
        if serde_json::to_vec(response)?.len() > MAX_RESPONSE_BYTES {
            return Err(StorageError::Invariant(format!(
                "receipt response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(existing) = receipt_by_id(&tx, operation_id)? else {
            tx.commit()?;
            return Err(StorageError::Invariant(format!(
                "cannot complete unknown receipt `{operation_id}`"
            )));
        };
        if existing.status != ReceiptState::Pending {
            tx.commit()?;
            return Ok(());
        }
        let completed = format_time(OffsetDateTime::now_utc())?;
        let response_json = serde_json::to_string(response)?;
        tx.execute(
            "UPDATE mutation_receipts SET status='completed', response_json=?1, completed_at=?2 \
             WHERE operation_id=?3 AND status='pending'",
            params![response_json, completed, operation_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Fetch a receipt by exact operation id. `None` is an explicit authoritative miss.
    pub fn get_receipt(&self, operation_id: &str) -> Result<Option<MutationReceipt>> {
        let conn = self.connection()?;
        receipt_by_id(&conn, operation_id)
    }

    /// Durably attach caller-supplied metadata (`details`) to a **pending** receipt.
    ///
    /// Used by handlers that must survive the crash window *after* committing the mutation but
    /// *before* recording its side effect (e.g. a drawer delete commits, then the `drawer_deleted`
    /// change-event append never lands): the metadata is persisted **before** the mutation runs,
    /// so a retry that recovers the pending receipt can converge using it instead of re-reading
    /// state the mutation already destroyed. Commits in its own transaction, so it is durable as
    /// soon as it returns. Rejecting a completed receipt is an invariant error — details can only
    /// be attached while the mutation is still in flight.
    pub fn set_receipt_details(&self, operation_id: &str, details: &Value) -> Result<()> {
        bounded_identifier(operation_id, "operation_id")?;
        let details_json = serde_json::to_string(details)?;
        if details_json.len() > MAX_RESPONSE_BYTES {
            return Err(StorageError::Invariant(format!(
                "receipt details exceed {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(existing) = receipt_by_id(&tx, operation_id)? else {
            tx.commit()?;
            return Err(StorageError::Invariant(format!(
                "cannot annotate unknown receipt `{operation_id}`"
            )));
        };
        if existing.status != ReceiptState::Pending {
            tx.commit()?;
            return Err(StorageError::Invariant(format!(
                "cannot annotate completed receipt `{operation_id}`"
            )));
        }
        tx.execute(
            "UPDATE mutation_receipts SET details_json=?1 \
             WHERE operation_id=?2 AND status='pending'",
            params![details_json, operation_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// List all pending receipts, oldest first — the startup-reconciliation feed.
    pub fn pending_receipts(&self) -> Result<Vec<MutationReceipt>> {
        let conn = self.connection()?;
        collect_receipts(
            &conn,
            "SELECT operation_id, operation_kind, request_hash, target_id, status, response_json, \
             created_at, completed_at, details_json FROM mutation_receipts WHERE status='pending' \
             ORDER BY created_at ASC",
        )
    }

    fn connection(&self) -> Result<Connection> {
        crate::coordination::open_palace_connection(&self.path)
    }
}

fn receipt_by_id(conn: &Connection, operation_id: &str) -> Result<Option<MutationReceipt>> {
    conn.prepare(
        "SELECT operation_id, operation_kind, request_hash, target_id, status, response_json, \
         created_at, completed_at, details_json FROM mutation_receipts WHERE operation_id=?1",
    )?
    .query_row([operation_id], receipt_row)
    .optional()
    .map_err(Into::into)
}

fn collect_receipts(conn: &Connection, sql: &str) -> Result<Vec<MutationReceipt>> {
    let mut statement = conn.prepare(sql)?;
    let rows = statement.query_map([], receipt_row)?.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn receipt_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MutationReceipt> {
    let status: String = row.get(4)?;
    let response: Option<String> = row.get(5)?;
    let details: Option<String> = row.get(8)?;
    let created: String = row.get(6)?;
    let completed: Option<String> = row.get(7)?;
    Ok(MutationReceipt {
        operation_id: row.get(0)?,
        operation_kind: row.get(1)?,
        request_hash: row.get(2)?,
        target_id: row.get(3)?,
        status: ReceiptState::parse(&status).map_err(sql_conv)?,
        response: response
            .map(|value| serde_json::from_str(&value))
            .transpose()
            .map_err(sql_conv)?,
        details: details.map(|value| serde_json::from_str(&value)).transpose().map_err(sql_conv)?,
        created_at: parse_time(created).map_err(sql_conv)?,
        completed_at: parse_time_opt(completed).map_err(sql_conv)?,
    })
}

fn sql_conv<E: std::fmt::Display>(error: E) -> rusqlite::Error {
    rusqlite::Error::InvalidColumnType(
        0,
        format!("invalid stored receipt value: {error}"),
        rusqlite::types::Type::Text,
    )
}

fn bounded_identifier(value: &str, name: &str) -> Result<()> {
    if value.len() > MAX_IDENTIFIER_BYTES {
        Err(StorageError::Invariant(format!("{name} must be at most {MAX_IDENTIFIER_BYTES} bytes")))
    } else {
        nonempty_identifier(value, name)
    }
}

fn nonempty_identifier(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant(format!("{name} must not be empty")))
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

impl ReceiptState {
    fn parse(value: &str) -> std::result::Result<ReceiptState, String> {
        match value {
            "pending" => Ok(ReceiptState::Pending),
            "completed" => Ok(ReceiptState::Completed),
            other => Err(format!("unknown receipt state `{other}`")),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        MutationReceiptStore, NewReceipt, RECEIPT_KIND_DRAWER_ADD, ReceiptOutcome, ReceiptState,
    };

    fn store() -> (MutationReceiptStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = MutationReceiptStore::new(dir.path().join("storage.sqlite3"));
        store.ensure_schema().unwrap();
        (store, dir)
    }

    fn receipt(op: &str, hash: &str) -> NewReceipt {
        NewReceipt {
            operation_id: op.to_owned(),
            operation_kind: RECEIPT_KIND_DRAWER_ADD.to_owned(),
            request_hash: hash.to_owned(),
            target_id: "drawer_x".to_owned(),
        }
    }

    #[test]
    fn fresh_then_complete_then_replay() {
        let (store, _dir) = store();

        let first = store.begin_receipt(&receipt("op-1", "hash-a")).unwrap();
        let ReceiptOutcome::Fresh(fresh) = first else {
            panic!("expected Fresh, got {first:?}");
        };
        assert_eq!(fresh.status, ReceiptState::Pending);
        assert_eq!(fresh.response, None);

        store.complete_receipt("op-1", &json!({"success": true, "drawer_id": "drawer_x"})).unwrap();

        let completed = store.get_receipt("op-1").unwrap().unwrap();
        assert_eq!(completed.status, ReceiptState::Completed);
        assert_eq!(completed.response, Some(json!({"success": true, "drawer_id": "drawer_x"})));

        let replay = store.begin_receipt(&receipt("op-1", "hash-a")).unwrap();
        let ReceiptOutcome::Replay(replayed) = replay else {
            panic!("expected Replay, got {replay:?}");
        };
        assert_eq!(replayed.status, ReceiptState::Completed);
        assert_eq!(replayed.response, Some(json!({"success": true, "drawer_id": "drawer_x"})));
    }

    #[test]
    fn same_operation_different_request_is_conflict() {
        let (store, _dir) = store();

        let first = store.begin_receipt(&receipt("op-2", "hash-a")).unwrap();
        assert!(matches!(first, ReceiptOutcome::Fresh(_)));
        store.complete_receipt("op-2", &json!({"success": true})).unwrap();

        let conflict = store.begin_receipt(&receipt("op-2", "hash-different")).unwrap();
        let ReceiptOutcome::Conflict { operation_id } = &conflict else {
            panic!("expected Conflict, got {conflict:?}");
        };
        assert_eq!(operation_id, "op-2");
    }

    #[test]
    fn pending_attempt_survives_reopen_and_is_recoverable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("storage.sqlite3");
        {
            let store = MutationReceiptStore::new(&path);
            store.ensure_schema().unwrap();
            let first = store.begin_receipt(&receipt("op-3", "hash-a")).unwrap();
            assert!(matches!(first, ReceiptOutcome::Fresh(_)));
        }
        {
            // A fresh store on the same file is what a process restart looks like.
            let store = MutationReceiptStore::new(&path);
            store.ensure_schema().unwrap();

            let pending = store.pending_receipts().unwrap();
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0].operation_id, "op-3");
            assert_eq!(pending[0].status, ReceiptState::Pending);

            let recover = store.begin_receipt(&receipt("op-3", "hash-a")).unwrap();
            let ReceiptOutcome::Recover(recovered) = &recover else {
                panic!("expected Recover, got {recover:?}");
            };
            assert_eq!(recovered.target_id, "drawer_x");

            // A *different* request against the pending receipt is still a conflict.
            let conflict = store.begin_receipt(&receipt("op-3", "hash-different")).unwrap();
            assert!(matches!(conflict, ReceiptOutcome::Conflict { .. }));
        }
    }

    #[test]
    fn complete_is_idempotent_and_keeps_first_response() {
        let (store, _dir) = store();

        store.begin_receipt(&receipt("op-4", "hash-a")).unwrap();
        store.complete_receipt("op-4", &json!({"response": 1})).unwrap();
        store.complete_receipt("op-4", &json!({"response": 2})).unwrap();

        let receipt = store.get_receipt("op-4").unwrap().unwrap();
        assert_eq!(receipt.response, Some(json!({"response": 1})));
    }

    #[test]
    fn completing_an_unknown_receipt_is_an_error() {
        let (store, _dir) = store();
        let err = store.complete_receipt("op-nope", &json!({"success": true})).unwrap_err();
        assert!(err.to_string().contains("unknown receipt"), "{err}");
    }

    #[test]
    fn set_details_persists_durably_and_rejects_late_updates() {
        let (store, _dir) = store();

        store.begin_receipt(&receipt("op-details-1", "hash-a")).unwrap();
        store
            .set_receipt_details("op-details-1", &json!({"wing": "wing_code", "room": "r"}))
            .unwrap();

        // Survives a store reopen (what a process restart looks like).
        {
            let reopened = MutationReceiptStore::new(_dir.path().join("storage.sqlite3"));
            reopened.ensure_schema().unwrap();
            let receipt = reopened.get_receipt("op-details-1").unwrap().unwrap();
            assert_eq!(receipt.details, Some(json!({"wing": "wing_code", "room": "r"})));
        }

        // A completed receipt can no longer be annotated.
        store.complete_receipt("op-details-1", &json!({"success": true})).unwrap();
        let err =
            store.set_receipt_details("op-details-1", &json!({"wing": "wing_code"})).unwrap_err();
        assert!(err.to_string().contains("completed"), "{err}");

        // Unknown receipts are rejected outright.
        let err = store.set_receipt_details("op-nope", &json!({"wing": "wing_code"})).unwrap_err();
        assert!(err.to_string().contains("unknown"), "{err}");
    }

    #[test]
    fn empty_operation_id_is_rejected() {
        let (store, _dir) = store();
        let err = store.begin_receipt(&receipt("", "hash-a")).unwrap_err();
        assert!(err.to_string().contains("operation_id"), "{err}");
    }

    #[test]
    fn target_id_is_not_limited_to_operation_metadata_size() {
        let (store, _dir) = store();
        let target_id = format!("drawer_{}", "x".repeat(512));
        let outcome = store
            .begin_receipt(&NewReceipt {
                operation_id: "op-long-target".to_owned(),
                operation_kind: RECEIPT_KIND_DRAWER_ADD.to_owned(),
                request_hash: "hash-long-target".to_owned(),
                target_id: target_id.clone(),
            })
            .unwrap();
        let ReceiptOutcome::Fresh(receipt) = outcome else {
            panic!("expected Fresh, got {outcome:?}");
        };
        assert_eq!(receipt.target_id, target_id);
    }
}
