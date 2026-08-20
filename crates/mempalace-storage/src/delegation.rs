//! Delegation-loop telemetry storage (Phase 2 coordination).
//!
//! Records what a delegated run declared it was allowed to spend, what it actually did, and why
//! it stopped — without storing full transcripts. Mirrors the revisioned-head-plus-append-only-log
//! shape of [`crate::coordination`]. See `docs/Coordination-Phase-2-Design.md`.
//!
//! MemPalace stores declared budgets and observed checkpoints; the host agent runtime enforces
//! budgets during execution. Nothing here refuses work for exceeding a budget. Depth and fan-out
//! are nevertheless *derived* from the span tree rather than trusted from the caller, so the
//! recorded numbers a host compares against its budget cannot silently drift.

use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::types::RevisionedWrite;
use crate::{Result, StorageError};

/// Checkpoint summaries are deliberately far smaller than the 1 MiB coordination payload cap.
/// A checkpoint is a note about what happened, not the thing that happened: anything larger
/// belongs in an immutable artifact, referenced by ID.
const MAX_SUMMARY_BYTES: usize = 8 * 1024;
/// Bounds total checkpoint content per span, not just each individual checkpoint. Without this,
/// a caller could still persist an arbitrarily large transcript by chunking it into many
/// under-the-cap checkpoints, defeating the point of the per-checkpoint bound.
const MAX_SPAN_SUMMARY_BYTES: usize = 256 * 1024;
const MAX_KEY_BYTES: usize = 256;
const MAX_BUDGET_BYTES: usize = 64 * 1024;

/// Lifecycle state of a delegated run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanStatus {
    /// The delegated run is in progress.
    Running,
    /// The run finished and produced its intended outcome.
    Completed,
    /// The run ended in an error.
    Failed,
    /// The run was cancelled by an actor.
    Cancelled,
    /// The run passed its deadline without completing.
    Expired,
}

impl SpanStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            _ => Err(StorageError::Invariant(format!("unknown span status `{value}`"))),
        }
    }
    fn terminal(self) -> bool {
        !matches!(self, Self::Running)
    }
}

/// Why a delegated run stopped. Recorded so budget exhaustion and early stops stay visible
/// rather than being inferred from an absent result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    /// Finished its objective.
    Completed,
    /// A declared token, time, turn, or tool-call budget ran out.
    BudgetExhausted,
    /// The host refused to delegate deeper.
    MaxDepthReached,
    /// The host refused to fan out wider.
    MaxFanOutReached,
    /// Cancelled by an actor.
    Cancelled,
    /// Ended in an error.
    Error,
    /// A human intervened and stopped the run.
    HumanStop,
}

impl StopReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::BudgetExhausted => "budget_exhausted",
            Self::MaxDepthReached => "max_depth_reached",
            Self::MaxFanOutReached => "max_fan_out_reached",
            Self::Cancelled => "cancelled",
            Self::Error => "error",
            Self::HumanStop => "human_stop",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "completed" => Ok(Self::Completed),
            "budget_exhausted" => Ok(Self::BudgetExhausted),
            "max_depth_reached" => Ok(Self::MaxDepthReached),
            "max_fan_out_reached" => Ok(Self::MaxFanOutReached),
            "cancelled" => Ok(Self::Cancelled),
            "error" => Ok(Self::Error),
            "human_stop" => Ok(Self::HumanStop),
            _ => Err(StorageError::Invariant(format!("unknown stop reason `{value}`"))),
        }
    }
}

/// What a checkpoint records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointType {
    /// One agent turn.
    Turn,
    /// One tool invocation.
    ToolCall,
    /// An observed token-usage reading.
    TokenUsage,
    /// A retry of previously attempted work.
    Retry,
    /// A human approved or rejected something.
    HumanApproval,
    /// A worker claimed the underlying task.
    Claim,
    /// Work was handed to another agent.
    Handoff,
}

impl CheckpointType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Turn => "turn",
            Self::ToolCall => "tool_call",
            Self::TokenUsage => "token_usage",
            Self::Retry => "retry",
            Self::HumanApproval => "human_approval",
            Self::Claim => "claim",
            Self::Handoff => "handoff",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "turn" => Ok(Self::Turn),
            "tool_call" => Ok(Self::ToolCall),
            "token_usage" => Ok(Self::TokenUsage),
            "retry" => Ok(Self::Retry),
            "human_approval" => Ok(Self::HumanApproval),
            "claim" => Ok(Self::Claim),
            "handoff" => Ok(Self::Handoff),
            _ => Err(StorageError::Invariant(format!("unknown checkpoint type `{value}`"))),
        }
    }
}

/// Input for an idempotent delegation span.
///
/// `depth` and `fan_out_index` are deliberately absent: both are derived from the span tree so
/// the recorded delegation shape cannot disagree with reality.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewSpan {
    /// Coordination task this run is executing. Required: the task tree is the single source of
    /// truth for what work exists.
    pub task_id: String,
    /// Parent span when this run was delegated by another run; absent for a root span.
    #[serde(default)]
    pub parent_span_id: Option<String>,
    /// Actor that delegated the work.
    pub delegator: String,
    /// Actor performing the work.
    pub delegate: String,
    /// Declared budgets. Stored, never enforced here.
    #[serde(default)]
    pub budgets: Option<Value>,
    /// Deduplication key, scoped to `delegator`.
    pub idempotency_key: String,
}

/// Authoritative delegation span record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Span {
    pub span_id: String,
    pub task_id: String,
    pub parent_span_id: Option<String>,
    pub delegator: String,
    pub delegate: String,
    /// Distance from the root span, derived as `parent.depth + 1`.
    pub depth: i64,
    /// Position among the parent's children at creation time, derived from the sibling count.
    pub fan_out_index: i64,
    pub budgets: Option<Value>,
    pub status: SpanStatus,
    pub stop_reason: Option<StopReason>,
    /// Actor that closed the span, present once it is terminal.
    pub closed_by: Option<String>,
    pub revision: i64,
    #[serde(with = "time::serde::rfc3339")]
    pub started_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub ended_at: Option<OffsetDateTime>,
}

/// Input for an idempotent checkpoint append.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewCheckpoint {
    pub span_id: String,
    pub checkpoint_type: CheckpointType,
    /// Bounded note about what happened. Never a full transcript.
    pub summary: String,
    /// Artifact holding anything too large for `summary`.
    #[serde(default)]
    pub artifact_ref: Option<String>,
    pub actor: String,
    pub idempotency_key: String,
}

/// Append-only checkpoint record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub checkpoint_id: String,
    pub sequence: i64,
    pub span_id: String,
    pub checkpoint_type: CheckpointType,
    pub summary: String,
    pub artifact_ref: Option<String>,
    pub actor: String,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
}

/// One span plus its checkpoints, as it appears in an exported trace.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TraceNode {
    pub span: Span,
    pub checkpoints: Vec<Checkpoint>,
}

/// A delegated run reconstructed from durable state, without any transcript.
///
/// Flattened rather than nested: every descendant span carries `parent_span_id`, so a consumer
/// can rebuild the tree without this type imposing a recursion depth.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trace {
    pub root_span_id: String,
    pub nodes: Vec<TraceNode>,
}

/// SQLite-backed delegation telemetry repository.
#[derive(Debug, Clone)]
pub struct DelegationStore {
    path: PathBuf,
}

impl DelegationStore {
    /// Open delegation telemetry in the palace's operational SQLite database.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self { path: path.as_ref().to_path_buf() }
    }

    /// Install delegation telemetry tables and indexes.
    pub fn ensure_schema(&self) -> Result<()> {
        let conn = self.connection()?;
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS delegation_spans (
 span_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, parent_span_id TEXT, delegator TEXT NOT NULL,
 delegate TEXT NOT NULL, depth INTEGER NOT NULL, fan_out_index INTEGER NOT NULL, budgets_json TEXT,
 status TEXT NOT NULL, stop_reason TEXT, closed_by TEXT, revision INTEGER NOT NULL,
 idempotency_key TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT,
 UNIQUE(delegator, idempotency_key),
 FOREIGN KEY(task_id) REFERENCES coordination_tasks(task_id),
 FOREIGN KEY(parent_span_id) REFERENCES delegation_spans(span_id));
CREATE INDEX IF NOT EXISTS idx_delegation_spans_task ON delegation_spans(task_id, started_at);
CREATE INDEX IF NOT EXISTS idx_delegation_spans_parent ON delegation_spans(parent_span_id);
CREATE TABLE IF NOT EXISTS delegation_checkpoints (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT, checkpoint_id TEXT UNIQUE NOT NULL, span_id TEXT NOT NULL,
 checkpoint_type TEXT NOT NULL, summary TEXT NOT NULL, artifact_ref TEXT, actor TEXT NOT NULL,
 idempotency_key TEXT NOT NULL, occurred_at TEXT NOT NULL, UNIQUE(actor, idempotency_key),
 FOREIGN KEY(span_id) REFERENCES delegation_spans(span_id),
 FOREIGN KEY(artifact_ref) REFERENCES coordination_artifacts(artifact_id));
CREATE INDEX IF NOT EXISTS idx_delegation_checkpoints_span
 ON delegation_checkpoints(span_id, sequence);
"#,
        )?;
        Ok(())
    }

    /// Start a delegation span, or return the prior committed span for an idempotency replay.
    ///
    /// `depth` and `fan_out_index` are derived from the parent, never taken from the caller.
    /// Exceeding a declared budget is *recorded*, not refused: enforcement is the host's job.
    pub fn start_span(&self, input: &NewSpan) -> Result<Span> {
        validate_actor(&input.delegator, "delegator")?;
        validate_actor(&input.delegate, "delegate")?;
        validate_key(&input.idempotency_key)?;
        if let Some(budgets) = &input.budgets {
            bounded_budget(budgets)?;
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(span) = find_span_by_key(&tx, &input.delegator, &input.idempotency_key)? {
            tx.commit()?;
            return Ok(span);
        }
        require_task(&tx, &input.task_id)?;
        let (depth, fan_out_index) = match &input.parent_span_id {
            Some(parent_id) => {
                let parent = require_span(&tx, parent_id)?;
                // A child span must stay inside its parent's task: otherwise a task-A trace can
                // pick up a task-B descendant, and list_spans_for_task (which filters strictly on
                // task_id) disagrees with trace() (which walks parent_span_id with no task
                // filter) about what belongs to which run.
                if parent.task_id != input.task_id {
                    return Err(StorageError::Invariant(format!(
                        "span `{parent_id}` belongs to task `{}`, not `{}`",
                        parent.task_id, input.task_id
                    )));
                }
                // A closed run should not keep growing. Without this, a new child span could be
                // created under an already-terminal parent — the same guarantee close_span's own
                // terminal guard enforces for the span being closed.
                if parent.status.terminal() {
                    return Err(StorageError::Invariant(format!(
                        "span `{parent_id}` is already {} and cannot gain new children",
                        parent.status.as_str()
                    )));
                }
                let siblings: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM delegation_spans WHERE parent_span_id=?1",
                    [parent_id],
                    |r| r.get(0),
                )?;
                (parent.depth + 1, siblings)
            }
            None => {
                let roots: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM delegation_spans WHERE parent_span_id IS NULL AND task_id=?1",
                    [&input.task_id],
                    |r| r.get(0),
                )?;
                (0, roots)
            }
        };
        let now = OffsetDateTime::now_utc();
        let id = format!("span_{}", Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO delegation_spans(span_id,task_id,parent_span_id,delegator,delegate,depth,\
             fan_out_index,budgets_json,status,stop_reason,closed_by,revision,idempotency_key,\
             started_at,ended_at)\
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'running',NULL,NULL,0,?9,?10,NULL)",
            params![
                id,
                input.task_id,
                input.parent_span_id,
                input.delegator,
                input.delegate,
                depth,
                fan_out_index,
                input.budgets.as_ref().map(serde_json::to_string).transpose()?,
                input.idempotency_key,
                format_time(now)?,
            ],
        )?;
        let span = get_span_tx(&tx, &id)?
            .ok_or_else(|| StorageError::Invariant("started span disappeared".into()))?;
        tx.commit()?;
        Ok(span)
    }

    /// Retrieve a span by exact ID. `None` is an explicit authoritative miss.
    pub fn get_span(&self, id: &str) -> Result<Option<Span>> {
        let conn = self.connection()?;
        get_span_conn(&conn, id)
    }

    /// List every span recorded against one task, oldest first.
    ///
    /// More than one root span for the same task is how repeated delegation of already-delegated
    /// work becomes visible rather than silent.
    pub fn list_spans_for_task(&self, task_id: &str) -> Result<Vec<Span>> {
        let conn = self.connection()?;
        let mut statement =
            conn.prepare(&format!("{SPAN_COLUMNS} WHERE task_id=?1 ORDER BY started_at, span_id"))?;
        statement
            .query_map([task_id], span_row)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Append a checkpoint, or return the prior committed checkpoint for an idempotency replay.
    pub fn append_checkpoint(&self, input: &NewCheckpoint) -> Result<Checkpoint> {
        validate_actor(&input.actor, "actor")?;
        validate_key(&input.idempotency_key)?;
        if input.summary.len() > MAX_SUMMARY_BYTES {
            return Err(StorageError::Invariant(format!(
                "checkpoint summary exceeds {MAX_SUMMARY_BYTES} bytes; \
                 store the full content as an artifact and reference it instead"
            )));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing) = find_checkpoint_by_key(&tx, &input.actor, &input.idempotency_key)? {
            tx.commit()?;
            return Ok(existing);
        }
        let span = require_span(&tx, &input.span_id)?;
        // A closed span's `ended_at` should be the true end of the run. Accepting late
        // checkpoints would let a trace keep changing after it claims to have stopped, and would
        // let occurred_at fall after ended_at, corrupting duration and ordering analysis.
        if span.status.terminal() {
            return Err(StorageError::Invariant(format!(
                "span `{}` is already {} and cannot gain new checkpoints",
                input.span_id,
                span.status.as_str()
            )));
        }
        if let Some(artifact_id) = &input.artifact_ref {
            // The FK only proves the artifact exists, not that it belongs to this run. Without
            // this check, a trace export could point a consumer at another task's content.
            let artifact_task_id: Option<String> = tx
                .query_row(
                    "SELECT task_id FROM coordination_artifacts WHERE artifact_id=?1",
                    [artifact_id],
                    |r| r.get(0),
                )
                .optional()?;
            match artifact_task_id {
                Some(task_id) if task_id == span.task_id => {}
                Some(task_id) => {
                    return Err(StorageError::Invariant(format!(
                        "artifact `{artifact_id}` belongs to task `{task_id}`, not span task `{}`",
                        span.task_id
                    )));
                }
                None => {
                    return Err(StorageError::Invariant(format!(
                        "artifact `{artifact_id}` not found"
                    )));
                }
            }
        }
        // The per-checkpoint cap alone does not bound a transcript split across many checkpoints.
        // A cumulative cap is what actually makes storing one structurally impractical.
        let existing_summary_bytes: i64 = tx.query_row(
            "SELECT COALESCE(SUM(LENGTH(summary)),0) FROM delegation_checkpoints WHERE span_id=?1",
            [&input.span_id],
            |r| r.get(0),
        )?;
        if existing_summary_bytes as usize + input.summary.len() > MAX_SPAN_SUMMARY_BYTES {
            return Err(StorageError::Invariant(format!(
                "span `{}` has reached its {MAX_SPAN_SUMMARY_BYTES}-byte cumulative checkpoint \
                 summary bound; store further content as an artifact instead",
                input.span_id
            )));
        }
        let now = OffsetDateTime::now_utc();
        let id = format!("checkpoint_{}", Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO delegation_checkpoints(checkpoint_id,span_id,checkpoint_type,summary,\
             artifact_ref,actor,idempotency_key,occurred_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                id,
                input.span_id,
                input.checkpoint_type.as_str(),
                input.summary,
                input.artifact_ref,
                input.actor,
                input.idempotency_key,
                format_time(now)?,
            ],
        )?;
        let checkpoint = get_checkpoint_tx(&tx, &id)?
            .ok_or_else(|| StorageError::Invariant("appended checkpoint disappeared".into()))?;
        tx.commit()?;
        Ok(checkpoint)
    }

    /// Retrieve a checkpoint by exact ID. `None` is an explicit authoritative miss.
    pub fn get_checkpoint(&self, id: &str) -> Result<Option<Checkpoint>> {
        let conn = self.connection()?;
        get_checkpoint_conn(&conn, id)
    }

    /// Close a running span with a status and an explicit stop reason, using compare-and-swap
    /// revision semantics. Terminal spans reject further transitions.
    pub fn close_span(
        &self,
        span_id: &str,
        expected_revision: i64,
        status: SpanStatus,
        stop_reason: StopReason,
        actor: &str,
    ) -> Result<RevisionedWrite<Span>> {
        validate_actor(actor, "actor")?;
        if !status.terminal() {
            return Err(StorageError::Invariant(
                "close_span requires a terminal status; `running` is not one".into(),
            ));
        }
        if !status_matches_stop_reason(status, stop_reason) {
            return Err(StorageError::Invariant(format!(
                "status `{}` is not consistent with stop_reason `{}`",
                status.as_str(),
                stop_reason.as_str()
            )));
        }
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(current) = get_span_tx(&tx, span_id)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        if current.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(current.revision) });
        }
        if current.status.terminal() {
            return Err(StorageError::Invariant(format!(
                "span `{span_id}` is already {} and cannot be closed again",
                current.status.as_str()
            )));
        }
        let now = OffsetDateTime::now_utc();
        let updated = tx.execute(
            "UPDATE delegation_spans SET status=?2, stop_reason=?3, closed_by=?4, revision=?5, \
             ended_at=?6 WHERE span_id=?1 AND revision=?7",
            params![
                span_id,
                status.as_str(),
                stop_reason.as_str(),
                actor,
                current.revision + 1,
                format_time(now)?,
                expected_revision,
            ],
        )?;
        if updated != 1 {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        }
        let span = get_span_tx(&tx, span_id)?
            .ok_or_else(|| StorageError::Invariant("closed span disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(span))
    }

    /// Reconstruct a delegated run from durable state: the root span, every descendant span, and
    /// each of their checkpoints in order. No transcript is involved.
    pub fn trace(&self, root_span_id: &str) -> Result<Option<Trace>> {
        let conn = self.connection()?;
        let Some(root) = get_span_conn(&conn, root_span_id)? else {
            return Ok(None);
        };
        let mut nodes = Vec::new();
        let mut frontier = vec![root];
        while let Some(span) = frontier.pop() {
            let mut children = conn
                .prepare(&format!("{SPAN_COLUMNS} WHERE parent_span_id=?1 ORDER BY fan_out_index"))?
                .query_map([&span.span_id], span_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let checkpoints = conn
                .prepare(&format!("{CHECKPOINT_COLUMNS} WHERE span_id=?1 ORDER BY sequence"))?
                .query_map([&span.span_id], checkpoint_row)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            nodes.push(TraceNode { span, checkpoints });
            frontier.append(&mut children);
        }
        nodes.sort_by(|left, right| {
            left.span.depth.cmp(&right.span.depth).then(left.span.started_at.cmp(&right.span.started_at))
        });
        Ok(Some(Trace { root_span_id: root_span_id.to_owned(), nodes }))
    }

    fn connection(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)?;
        conn.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;",
        )?;
        Ok(conn)
    }
}

const SPAN_COLUMNS: &str = "SELECT span_id,task_id,parent_span_id,delegator,delegate,depth,\
fan_out_index,budgets_json,status,stop_reason,closed_by,revision,started_at,ended_at FROM delegation_spans";
const CHECKPOINT_COLUMNS: &str = "SELECT checkpoint_id,sequence,span_id,checkpoint_type,summary,\
artifact_ref,actor,occurred_at FROM delegation_checkpoints";

fn require_task(tx: &Transaction<'_>, task_id: &str) -> Result<()> {
    let exists: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM coordination_tasks WHERE task_id=?1)",
        [task_id],
        |r| r.get(0),
    )?;
    if exists {
        Ok(())
    } else {
        Err(StorageError::Invariant(format!("task `{task_id}` not found")))
    }
}
fn require_span(tx: &Transaction<'_>, span_id: &str) -> Result<Span> {
    get_span_tx(tx, span_id)?
        .ok_or_else(|| StorageError::Invariant(format!("span `{span_id}` not found")))
}
fn get_span_conn(conn: &Connection, id: &str) -> Result<Option<Span>> {
    conn.prepare(&format!("{SPAN_COLUMNS} WHERE span_id=?1"))?
        .query_row([id], span_row)
        .optional()
        .map_err(Into::into)
}
fn get_span_tx(tx: &Transaction<'_>, id: &str) -> Result<Option<Span>> {
    get_span_conn(tx, id)
}
fn find_span_by_key(tx: &Transaction<'_>, actor: &str, key: &str) -> Result<Option<Span>> {
    let id: Option<String> = tx
        .query_row(
            "SELECT span_id FROM delegation_spans WHERE delegator=?1 AND idempotency_key=?2",
            params![actor, key],
            |r| r.get(0),
        )
        .optional()?;
    id.map(|value| require_span(tx, &value)).transpose()
}
fn span_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Span> {
    let budgets: Option<String> = r.get(7)?;
    let status: String = r.get(8)?;
    let stop_reason: Option<String> = r.get(9)?;
    let started: String = r.get(12)?;
    let ended: Option<String> = r.get(13)?;
    Ok(Span {
        span_id: r.get(0)?,
        task_id: r.get(1)?,
        parent_span_id: r.get(2)?,
        delegator: r.get(3)?,
        delegate: r.get(4)?,
        depth: r.get(5)?,
        fan_out_index: r.get(6)?,
        budgets: budgets.map(|v| serde_json::from_str(&v)).transpose().map_err(sql_conv)?,
        status: SpanStatus::parse(&status).map_err(sql_conv)?,
        stop_reason: stop_reason.map(|v| StopReason::parse(&v)).transpose().map_err(sql_conv)?,
        closed_by: r.get(10)?,
        revision: r.get(11)?,
        started_at: parse_time(started).map_err(sql_conv)?,
        ended_at: parse_time_opt(ended).map_err(sql_conv)?,
    })
}
fn get_checkpoint_conn(conn: &Connection, id: &str) -> Result<Option<Checkpoint>> {
    conn.prepare(&format!("{CHECKPOINT_COLUMNS} WHERE checkpoint_id=?1"))?
        .query_row([id], checkpoint_row)
        .optional()
        .map_err(Into::into)
}
fn get_checkpoint_tx(tx: &Transaction<'_>, id: &str) -> Result<Option<Checkpoint>> {
    get_checkpoint_conn(tx, id)
}
fn find_checkpoint_by_key(
    tx: &Transaction<'_>,
    actor: &str,
    key: &str,
) -> Result<Option<Checkpoint>> {
    let id: Option<String> = tx
        .query_row(
            "SELECT checkpoint_id FROM delegation_checkpoints WHERE actor=?1 AND idempotency_key=?2",
            params![actor, key],
            |r| r.get(0),
        )
        .optional()?;
    id.map(|value| {
        get_checkpoint_tx(tx, &value)?
            .ok_or_else(|| StorageError::Invariant("checkpoint key points to missing row".into()))
    })
    .transpose()
}
fn checkpoint_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Checkpoint> {
    let kind: String = r.get(3)?;
    let at: String = r.get(7)?;
    Ok(Checkpoint {
        checkpoint_id: r.get(0)?,
        sequence: r.get(1)?,
        span_id: r.get(2)?,
        checkpoint_type: CheckpointType::parse(&kind).map_err(sql_conv)?,
        summary: r.get(4)?,
        artifact_ref: r.get(5)?,
        actor: r.get(6)?,
        occurred_at: parse_time(at).map_err(sql_conv)?,
    })
}

/// Whether a terminal status is a coherent outcome for a stop reason. Without this, contradictory
/// records such as `status: completed` with `stop_reason: error` would make outcome and budget
/// telemetry ambiguous for every trace consumer.
fn status_matches_stop_reason(status: SpanStatus, reason: StopReason) -> bool {
    matches!(
        (status, reason),
        (SpanStatus::Completed, StopReason::Completed)
            | (
                SpanStatus::Failed,
                StopReason::Error
                    | StopReason::BudgetExhausted
                    | StopReason::MaxDepthReached
                    | StopReason::MaxFanOutReached
            )
            | (SpanStatus::Cancelled, StopReason::Cancelled | StopReason::HumanStop)
            | (SpanStatus::Expired, StopReason::BudgetExhausted)
    )
}

fn validate_actor(value: &str, name: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(StorageError::Invariant(format!("{name} must not be empty")))
    } else {
        Ok(())
    }
}
fn validate_key(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > MAX_KEY_BYTES {
        Err(StorageError::Invariant("idempotency key must contain 1..=256 bytes".into()))
    } else {
        Ok(())
    }
}
fn bounded_budget(value: &Value) -> Result<()> {
    if serde_json::to_vec(value)?.len() > MAX_BUDGET_BYTES {
        Err(StorageError::Invariant("budgets exceed 64 KiB".into()))
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
fn sql_conv<E: std::error::Error + Send + Sync + 'static>(error: E) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordination::{CoordinationStore, NewTask};
    use tempfile::TempDir;

    fn store() -> (TempDir, CoordinationStore, DelegationStore) {
        let dir = TempDir::new().expect("temp dir");
        let path = dir.path().join("storage.sqlite3");
        let coordination = CoordinationStore::new(&path);
        coordination.ensure_schema().expect("coordination schema");
        let delegation = DelegationStore::new(&path);
        delegation.ensure_schema().expect("delegation schema");
        (dir, coordination, delegation)
    }

    fn task(c: &CoordinationStore, key: &str) -> String {
        c.create_task(&NewTask {
            title: "research".into(),
            description: "do it".into(),
            created_by: "manager".into(),
            wing: "wing_test".into(),
            idempotency_key: key.into(),
            parent_id: None,
            dependencies: vec![],
            budget: None,
            expires_at: None,
        })
        .expect("task")
        .task_id
    }

    fn root_span(d: &DelegationStore, task_id: &str, key: &str) -> Span {
        d.start_span(&NewSpan {
            task_id: task_id.to_owned(),
            parent_span_id: None,
            delegator: "manager".into(),
            delegate: "worker-1".into(),
            budgets: Some(serde_json::json!({"max_depth":3,"max_tokens":10_000})),
            idempotency_key: key.into(),
        })
        .expect("root span")
    }

    #[test]
    fn spans_are_idempotent_and_require_a_real_task() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let first = root_span(&d, &task_id, "span-1");
        let replay = root_span(&d, &task_id, "span-1");
        assert_eq!(first, replay);
        assert_eq!(first.depth, 0);

        let orphan = d.start_span(&NewSpan {
            task_id: "task_missing".into(),
            parent_span_id: None,
            delegator: "manager".into(),
            delegate: "worker-1".into(),
            budgets: None,
            idempotency_key: "span-orphan".into(),
        });
        assert!(orphan.is_err(), "a span must reference a real task");
    }

    #[test]
    fn depth_and_fan_out_are_derived_not_trusted() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let root = root_span(&d, &task_id, "span-root");

        let child_a = d
            .start_span(&NewSpan {
                task_id: task_id.clone(),
                parent_span_id: Some(root.span_id.clone()),
                delegator: "worker-1".into(),
                delegate: "worker-2".into(),
                budgets: None,
                idempotency_key: "span-a".into(),
            })
            .expect("child a");
        let child_b = d
            .start_span(&NewSpan {
                task_id: task_id.clone(),
                parent_span_id: Some(root.span_id.clone()),
                delegator: "worker-1".into(),
                delegate: "worker-3".into(),
                budgets: None,
                idempotency_key: "span-b".into(),
            })
            .expect("child b");
        let grandchild = d
            .start_span(&NewSpan {
                task_id: task_id.clone(),
                parent_span_id: Some(child_a.span_id.clone()),
                delegator: "worker-2".into(),
                delegate: "worker-4".into(),
                budgets: None,
                idempotency_key: "span-c".into(),
            })
            .expect("grandchild");

        assert_eq!((child_a.depth, child_a.fan_out_index), (1, 0));
        assert_eq!((child_b.depth, child_b.fan_out_index), (1, 1));
        assert_eq!((grandchild.depth, grandchild.fan_out_index), (2, 0));
    }

    #[test]
    fn checkpoint_summaries_are_bounded_so_transcripts_cannot_be_stored() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let span = root_span(&d, &task_id, "span-1");
        let oversized = "x".repeat(MAX_SUMMARY_BYTES + 1);
        let rejected = d.append_checkpoint(&NewCheckpoint {
            span_id: span.span_id.clone(),
            checkpoint_type: CheckpointType::Turn,
            summary: oversized,
            artifact_ref: None,
            actor: "worker-1".into(),
            idempotency_key: "cp-big".into(),
        });
        assert!(rejected.is_err(), "an oversized summary must be refused");

        let accepted = d
            .append_checkpoint(&NewCheckpoint {
                span_id: span.span_id.clone(),
                checkpoint_type: CheckpointType::ToolCall,
                summary: "called mempalace_search, 3 hits".into(),
                artifact_ref: None,
                actor: "worker-1".into(),
                idempotency_key: "cp-1".into(),
            })
            .expect("checkpoint");
        let replay = d
            .append_checkpoint(&NewCheckpoint {
                span_id: span.span_id,
                checkpoint_type: CheckpointType::ToolCall,
                summary: "called mempalace_search, 3 hits".into(),
                artifact_ref: None,
                actor: "worker-1".into(),
                idempotency_key: "cp-1".into(),
            })
            .expect("replayed checkpoint");
        assert_eq!(accepted, replay);
        assert_eq!(
            d.get_checkpoint(&accepted.checkpoint_id).expect("get").expect("present"),
            accepted
        );
        assert!(d.get_checkpoint("checkpoint_missing").expect("get").is_none());
    }

    #[test]
    fn closing_is_cas_and_records_an_explicit_stop_reason() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let span = root_span(&d, &task_id, "span-1");

        let stale = d
            .close_span(
                &span.span_id,
                span.revision + 7,
                SpanStatus::Completed,
                StopReason::Completed,
                "manager",
            )
            .expect("close call");
        assert!(matches!(stale, RevisionedWrite::Conflict { .. }));

        let RevisionedWrite::Applied(closed) = d
            .close_span(
                &span.span_id,
                span.revision,
                SpanStatus::Failed,
                StopReason::BudgetExhausted,
                "manager",
            )
            .expect("close call")
        else {
            panic!("expected applied")
        };
        assert_eq!(closed.status, SpanStatus::Failed);
        assert_eq!(closed.stop_reason, Some(StopReason::BudgetExhausted));
        assert!(closed.ended_at.is_some());

        // A terminal span cannot be closed again.
        assert!(
            d.close_span(
                &span.span_id,
                closed.revision,
                SpanStatus::Completed,
                StopReason::Completed,
                "manager",
            )
            .is_err()
        );
        // `running` is not a valid closing status.
        assert!(
            d.close_span(&span.span_id, closed.revision, SpanStatus::Running, StopReason::Completed, "manager")
                .is_err()
        );
    }

    #[test]
    fn a_delegated_run_reconstructs_from_durable_state_without_a_transcript() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let root = root_span(&d, &task_id, "span-root");
        let child = d
            .start_span(&NewSpan {
                task_id: task_id.clone(),
                parent_span_id: Some(root.span_id.clone()),
                delegator: "worker-1".into(),
                delegate: "worker-2".into(),
                budgets: None,
                idempotency_key: "span-child".into(),
            })
            .expect("child");
        for (index, span_id) in [(0, &root.span_id), (1, &child.span_id)] {
            d.append_checkpoint(&NewCheckpoint {
                span_id: span_id.clone(),
                checkpoint_type: CheckpointType::Turn,
                summary: format!("turn {index}"),
                artifact_ref: None,
                actor: "worker-1".into(),
                idempotency_key: format!("cp-{index}"),
            })
            .expect("checkpoint");
        }
        d.close_span(&child.span_id, child.revision, SpanStatus::Completed, StopReason::Completed, "worker-1")
            .expect("close child");

        let trace = d.trace(&root.span_id).expect("trace").expect("present");
        assert_eq!(trace.nodes.len(), 2);
        assert_eq!(trace.nodes[0].span.span_id, root.span_id);
        assert_eq!(trace.nodes[0].span.depth, 0);
        assert_eq!(trace.nodes[1].span.parent_span_id.as_deref(), Some(root.span_id.as_str()));
        assert_eq!(trace.nodes[1].span.stop_reason, Some(StopReason::Completed));
        assert!(trace.nodes.iter().all(|node| node.checkpoints.len() == 1));
        assert!(d.trace("span_missing").expect("trace").is_none());
    }

    #[test]
    fn repeated_delegation_of_one_task_is_visible() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let first = root_span(&d, &task_id, "span-1");
        let second = d
            .start_span(&NewSpan {
                task_id: task_id.clone(),
                parent_span_id: None,
                delegator: "manager".into(),
                delegate: "worker-9".into(),
                budgets: None,
                idempotency_key: "span-2".into(),
            })
            .expect("second root");
        assert_eq!(second.fan_out_index, 1, "a second root span records the repeat");

        let spans = d.list_spans_for_task(&task_id).expect("spans");
        assert_eq!(spans.len(), 2);
        assert!(spans.iter().any(|span| span.span_id == first.span_id));
        assert!(spans.iter().any(|span| span.span_id == second.span_id));
    }

    #[test]
    fn a_child_span_must_belong_to_its_parents_task() {
        let (_dir, c, d) = store();
        let task_a = task(&c, "t-a");
        let task_b = task(&c, "t-b");
        let root_a = root_span(&d, &task_a, "span-root-a");

        let mismatched = d.start_span(&NewSpan {
            task_id: task_b,
            parent_span_id: Some(root_a.span_id.clone()),
            delegator: "worker-1".into(),
            delegate: "worker-2".into(),
            budgets: None,
            idempotency_key: "span-cross-task".into(),
        });
        assert!(mismatched.is_err(), "a child span must stay inside its parent's task");
    }

    #[test]
    fn a_terminal_span_cannot_gain_new_children_or_checkpoints() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let root = root_span(&d, &task_id, "span-root");
        d.close_span(&root.span_id, root.revision, SpanStatus::Completed, StopReason::Completed, "worker-1")
            .expect("close");

        let child = d.start_span(&NewSpan {
            task_id: task_id.clone(),
            parent_span_id: Some(root.span_id.clone()),
            delegator: "worker-1".into(),
            delegate: "worker-2".into(),
            budgets: None,
            idempotency_key: "span-late-child".into(),
        });
        assert!(child.is_err(), "a closed span must not gain new children");

        let checkpoint = d.append_checkpoint(&NewCheckpoint {
            span_id: root.span_id,
            checkpoint_type: CheckpointType::Turn,
            summary: "late arrival".into(),
            artifact_ref: None,
            actor: "worker-1".into(),
            idempotency_key: "cp-late".into(),
        });
        assert!(checkpoint.is_err(), "a closed span must not gain new checkpoints");
    }

    #[test]
    fn closing_actor_is_persisted() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let root = root_span(&d, &task_id, "span-root");
        let RevisionedWrite::Applied(closed) = d
            .close_span(&root.span_id, root.revision, SpanStatus::Completed, StopReason::Completed, "worker-9")
            .expect("close")
        else {
            panic!("expected applied")
        };
        assert_eq!(closed.closed_by.as_deref(), Some("worker-9"));
        let reloaded = d.get_span(&closed.span_id).expect("get").expect("present");
        assert_eq!(reloaded.closed_by.as_deref(), Some("worker-9"));
    }

    #[test]
    fn checkpoint_artifacts_must_belong_to_the_spans_task() {
        let (_dir, c, d) = store();
        let task_a = task(&c, "t-a");
        let task_b = task(&c, "t-b");
        let root_a = root_span(&d, &task_a, "span-root-a");
        let artifact_b = c
            .put_artifact(&crate::coordination::NewArtifact {
                task_id: task_b,
                created_by: "worker-1".into(),
                role: "partial_result".into(),
                media_type: "text/plain".into(),
                content: "belongs to task b".into(),
                idempotency_key: "artifact-b".into(),
            })
            .expect("artifact");

        let stray = d.append_checkpoint(&NewCheckpoint {
            span_id: root_a.span_id,
            checkpoint_type: CheckpointType::ToolCall,
            summary: "points at the wrong task's artifact".into(),
            artifact_ref: Some(artifact_b.artifact_id),
            actor: "worker-1".into(),
            idempotency_key: "cp-stray-artifact".into(),
        });
        assert!(stray.is_err(), "a checkpoint must not reference another task's artifact");
    }

    #[test]
    fn chunked_checkpoints_cannot_reassemble_an_unbounded_transcript() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let root = root_span(&d, &task_id, "span-root");
        let chunk = "x".repeat(MAX_SUMMARY_BYTES);
        let mut appended = 0usize;
        loop {
            let result = d.append_checkpoint(&NewCheckpoint {
                span_id: root.span_id.clone(),
                checkpoint_type: CheckpointType::Turn,
                summary: chunk.clone(),
                artifact_ref: None,
                actor: "worker-1".into(),
                idempotency_key: format!("cp-chunk-{appended}"),
            });
            match result {
                Ok(_) => appended += 1,
                Err(_) => break,
            }
            assert!(appended < 1000, "the cumulative cap should have been hit long before this");
        }
        assert!(
            appended * MAX_SUMMARY_BYTES < 10 * MAX_SPAN_SUMMARY_BYTES,
            "chunking must not be able to reassemble an effectively unbounded transcript"
        );
    }

    #[test]
    fn contradictory_status_and_stop_reason_are_rejected() {
        let (_dir, c, d) = store();
        let task_id = task(&c, "t1");
        let root = root_span(&d, &task_id, "span-root");
        let contradiction = d.close_span(
            &root.span_id,
            root.revision,
            SpanStatus::Completed,
            StopReason::Error,
            "worker-1",
        );
        assert!(contradiction.is_err(), "completed status paired with an error stop reason is incoherent");
    }
}
