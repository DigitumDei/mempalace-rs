//! Transactional, local-first coordination storage.

use std::path::{Path, PathBuf};

use mempalace_core::WingId;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{Result, StorageError};

const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_TASK_TEXT_BYTES: usize = 1024 * 1024;

/// Reserved wing name for coordination rows that existed before wings were introduced. Every
/// task and event created before this stage upgraded its schema reads back with this wing.
pub const UNSCOPED_WING: &str = "wing_unscoped";

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
        let conn = self.connection()?;
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
        add_column_if_missing(&conn, "coordination_tasks", "wing", "TEXT NOT NULL DEFAULT 'wing_unscoped'")?;
        add_column_if_missing(&conn, "coordination_events", "wing", "TEXT NOT NULL DEFAULT 'wing_unscoped'")?;
        // Wing-filtered `events()` calls are a continuously polled feed, so a full scan is a
        // real cost, not a theoretical one. The column is guaranteed to exist by this point on
        // both the fresh and the upgraded path.
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_coordination_events_wing ON coordination_events(wing, sequence);",
        )?;
        Ok(())
    }

    /// Create a task, or return the prior committed task for an idempotency replay.
    pub fn create_task(&self, input: &NewTask) -> Result<Task> {
        validate_key(&input.idempotency_key)?;
        validate_actor(&input.created_by)?;
        bounded_text(&input.title, "task title")?;
        bounded_text(&input.description, "task description")?;
        if let Some(budget) = &input.budget {
            bounded_json(budget)?;
        }
        let wing = WingId::normalized(&input.wing)?.to_string();
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(task) = find_task_by_key(&tx, &input.created_by, &input.idempotency_key)? {
            tx.commit()?;
            return Ok(task);
        }
        for dependency in &input.dependencies {
            require_task(&tx, dependency)?;
        }
        if let Some(parent) = &input.parent_id {
            require_task(&tx, parent)?;
        }
        let now = OffsetDateTime::now_utc();
        let id = format!("task_{}", Uuid::new_v4().simple());
        tx.execute("INSERT INTO coordination_tasks(task_id,title,description,state,revision,created_by,owner,parent_id,dependencies_json,budget_json,lease_expires_at,expires_at,idempotency_key,created_at,updated_at,wing) VALUES (?1,?2,?3,'pending',0,?4,NULL,?5,?6,?7,NULL,?8,?9,?10,?10,?11)", params![id,input.title,input.description,input.created_by,input.parent_id,serde_json::to_string(&input.dependencies)?,input.budget.as_ref().map(serde_json::to_string).transpose()?,format_time_opt(input.expires_at)?,input.idempotency_key,format_time(now)?,wing])?;
        append_event(
            &tx,
            "task",
            &id,
            Some(&id),
            &wing,
            "task_created",
            &input.created_by,
            None,
            Some(TaskState::Pending),
            Some(0),
            None,
            now,
        )?;
        let task = get_task_tx(&tx, &id)?
            .ok_or_else(|| StorageError::Invariant("created task disappeared".into()))?;
        tx.commit()?;
        Ok(task)
    }

    /// Retrieve a task by exact ID. `None` is an explicit authoritative miss.
    pub fn get_task(&self, id: &str) -> Result<Option<Task>> {
        get_task_conn(&self.connection()?, id)
    }

    /// Atomically claim a task revision, reclaiming an expired lease when needed.
    pub fn claim_task(
        &self,
        id: &str,
        worker: &str,
        expected_revision: i64,
        ttl: Duration,
    ) -> Result<Task> {
        validate_actor(worker)?;
        if ttl <= Duration::ZERO {
            return Err(StorageError::Invariant("lease ttl must be positive".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = require_task(&tx, id)?;
        let now = OffsetDateTime::now_utc();
        if task.revision != expected_revision {
            return Err(stale(expected_revision, task.revision));
        }
        if task.state.terminal() {
            return Err(StorageError::Invariant("terminal task cannot be claimed".into()));
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
            return Err(StorageError::Invariant("task has expired".into()));
        }
        if task.owner.as_deref().is_some_and(|owner| owner != worker)
            && task.lease_expires_at.is_some_and(|expiry| expiry > now)
        {
            return Err(StorageError::Invariant("task lease is held by another worker".into()));
        }
        let next = task.revision + 1;
        let expiry = now + ttl;
        let changed = tx.execute("UPDATE coordination_tasks SET state='running',revision=?2,owner=?3,lease_expires_at=?4,updated_at=?5 WHERE task_id=?1 AND revision=?6",params![id,next,worker,format_time(expiry)?,format_time(now)?,expected_revision])?;
        if changed != 1 {
            return Err(StorageError::Invariant("task changed while being claimed".into()));
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
        Ok(result)
    }

    /// Renew a live lease using compare-and-swap revision semantics.
    pub fn renew_lease(
        &self,
        id: &str,
        worker: &str,
        expected_revision: i64,
        ttl: Duration,
    ) -> Result<Task> {
        validate_actor(worker)?;
        if ttl <= Duration::ZERO {
            return Err(StorageError::Invariant("lease ttl must be positive".into()));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = require_task(&tx, id)?;
        let now = OffsetDateTime::now_utc();
        if task.revision != expected_revision {
            return Err(stale(expected_revision, task.revision));
        }
        if task.owner.as_deref() != Some(worker) {
            return Err(StorageError::Invariant("only the lease owner may renew".into()));
        }
        if task.lease_expires_at.is_none_or(|v| v <= now) {
            return Err(StorageError::Invariant("lease has expired".into()));
        }
        let next = task.revision + 1;
        tx.execute("UPDATE coordination_tasks SET revision=?2,lease_expires_at=?3,updated_at=?4 WHERE task_id=?1",params![id,next,format_time(now+ttl)?,format_time(now)?])?;
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
        Ok(result)
    }

    /// Transition lifecycle state using an expected revision.
    pub fn transition_task(
        &self,
        id: &str,
        actor: &str,
        expected_revision: i64,
        to: TaskState,
        details: Option<Value>,
    ) -> Result<Task> {
        validate_actor(actor)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let task = require_task(&tx, id)?;
        let now = OffsetDateTime::now_utc();
        if task.revision != expected_revision {
            return Err(stale(expected_revision, task.revision));
        }
        if !allowed_transition(task.state, to) {
            return Err(StorageError::Invariant(format!(
                "invalid transition {} -> {}",
                task.state.as_str(),
                to.as_str()
            )));
        }
        if task.owner.is_some()
            && task.owner.as_deref() != Some(actor)
            && to != TaskState::Cancelled
        {
            return Err(StorageError::Invariant("only the owner may transition this task".into()));
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
        Ok(result)
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
    ) -> Result<InboxPage> {
        let conn = self.connection()?;
        let requested = limit.clamp(1, 500);
        let mut sql = "SELECT m.message_id,m.sequence,m.task_id,m.sender,m.recipient,m.kind,m.payload_json,m.envelope_version,m.acknowledged_at,m.acknowledged_by,m.created_at FROM coordination_messages m".to_owned();
        let mut predicates = vec!["m.recipient=?1".to_owned(), "m.sequence>?2".to_owned()];
        let mut bindings: Vec<Box<dyn rusqlite::ToSql>> =
            vec![Box::new(recipient.to_owned()), Box::new(cursor.map_or(0, |c| c.0))];
        if let Some(wing) = wing {
            let normalized = WingId::normalized(wing)?.to_string();
            sql.push_str(" JOIN coordination_tasks t ON t.task_id=m.task_id");
            bindings.push(Box::new(normalized));
            predicates.push(format!("t.wing=?{}", bindings.len()));
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
            .ok_or_else(|| StorageError::Invariant(format!("message `{id}` not found")))?;
        if msg.recipient != actor {
            return Err(StorageError::Invariant("only the recipient may acknowledge".into()));
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
    ) -> Result<CoordinationEventPage> {
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
        let conn = Connection::open(&self.path)?;
        conn.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;",
        )?;
        Ok(conn)
    }
}

/// Add `column` to `table` with the given DDL fragment (type, nullability, default) unless it
/// already exists. `table` and `column` are always internal literals, never caller input, so
/// interpolating them into DDL text carries no injection risk; SQLite has no
/// `ADD COLUMN IF NOT EXISTS`, so the existence check is done by hand via `PRAGMA table_info`.
fn add_column_if_missing(conn: &Connection, table: &str, column: &str, ddl: &str) -> Result<()> {
    let mut statement = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let exists = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?
        .iter()
        .any(|name| name == column);
    if !exists {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {ddl};"))?;
    }
    Ok(())
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
fn stale(expected: i64, actual: i64) -> StorageError {
    StorageError::Invariant(format!("stale revision: expected {expected}, current {actual}"))
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
    get_task_tx(tx, id)?.ok_or_else(|| StorageError::Invariant(format!("task `{id}` not found")))
}
/// Look up just the owning task's wing, for events whose entity isn't a `Task` itself (e.g. a
/// message acknowledgement, which only has the `Message` on hand).
fn task_wing(tx: &Transaction<'_>, task_id: &str) -> Result<String> {
    tx.query_row("SELECT wing FROM coordination_tasks WHERE task_id=?1", [task_id], |r| r.get(0))
        .optional()?
        .ok_or_else(|| StorageError::Invariant(format!("task `{task_id}` not found")))
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
    fn claim_is_cas_and_expired_lease_is_reclaimable() {
        let (_d, s) = store();
        let t = task(&s);
        let claimed = s.claim_task(&t.task_id, "a", 0, Duration::milliseconds(1)).expect("claim");
        assert!(s.claim_task(&t.task_id, "b", 0, Duration::minutes(1)).is_err());
        std::thread::sleep(std::time::Duration::from_millis(5));
        let reclaimed =
            s.claim_task(&t.task_id, "b", claimed.revision, Duration::minutes(1)).expect("reclaim");
        assert_eq!(reclaimed.owner.as_deref(), Some("b"));
        let events = s.events(None, Some(&t.task_id), None, 20).expect("events");
        assert_eq!(events.events.len(), 3);
        let event = events.events.first().expect("event").clone();
        assert_eq!(s.get_event(&event.event_id).expect("exact event"), Some(event));
        assert_eq!(s.get_event("event_missing").expect("missing event"), None);
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
        let cancelled = s
            .transition_task(&child.task_id, "manager", child.revision, TaskState::Cancelled, None)
            .expect("cancel");
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
                store.claim_task(&task_id, worker, 0, Duration::minutes(1)).is_ok()
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
        let running = s.claim_task(&t.task_id, "worker", 0, Duration::minutes(1)).expect("claim");
        let waiting = s
            .transition_task(&t.task_id, "worker", running.revision, TaskState::InputRequired, None)
            .expect("input required");
        assert_eq!(waiting.state, TaskState::InputRequired);
        assert_eq!(waiting.owner.as_deref(), Some("worker"));
        assert!(s
            .transition_task(&t.task_id, "other", waiting.revision, TaskState::Running, None)
            .is_err());
        let running_again = s
            .transition_task(&t.task_id, "worker", waiting.revision, TaskState::Running, None)
            .expect("owner resumes task");
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
        let first = s.inbox("receiver", None, None, 1, false).expect("first inbox page");
        assert_eq!(first.messages.len(), 1);
        assert!(first.next_cursor.is_some());
        let last =
            s.inbox("receiver", first.next_cursor, None, 1, false).expect("last inbox page");
        assert_eq!(last.messages.len(), 1);
        assert_eq!(last.next_cursor, None);

        let first_events = s.events(None, Some(&t.task_id), None, 1).expect("first event page");
        assert_eq!(first_events.events.len(), 1);
        assert!(first_events.next_cursor.is_some());
        let mut cursor = first_events.next_cursor;
        while cursor.is_some() {
            let page = s.events(cursor, Some(&t.task_id), None, 500).expect("event page");
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
        let events = store.events(None, Some(&fresh.task_id), None, 10).expect("events");
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
        let claimed = s.claim_task(&t.task_id, "worker", t.revision, Duration::minutes(1)).expect("claim");
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
        let page = s.events(None, Some(&t.task_id), None, 50).expect("events");
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
        let alpha_events = s.events(None, None, Some("alpha"), 100).expect("alpha events");
        assert!(!alpha_events.events.is_empty());
        assert!(
            alpha_events.events.iter().all(|e| e.task_id.as_deref() == Some(alpha.task_id.as_str())),
            "an alpha wing filter must not leak beta's events"
        );

        let alpha_inbox = s.inbox("worker", None, Some("alpha"), 100, false).expect("alpha inbox");
        assert_eq!(alpha_inbox.messages.len(), 1);
        assert_eq!(alpha_inbox.messages[0].task_id, alpha.task_id);

        // The already-prefixed spelling must match too, on both sides of the earlier asymmetry.
        let beta_inbox =
            s.inbox("worker", None, Some("wing_beta"), 100, false).expect("beta inbox");
        assert_eq!(beta_inbox.messages.len(), 1);
        assert_eq!(beta_inbox.messages[0].task_id, beta.task_id);

        // No filter at all still returns both.
        let unfiltered = s.inbox("worker", None, None, 100, false).expect("unfiltered inbox");
        assert_eq!(unfiltered.messages.len(), 2);
    }
}
