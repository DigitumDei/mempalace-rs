//! Versioned, governed skill registry storage (Phase 2 coordination).
//!
//! Mirrors two proven patterns from this crate rather than inventing new ones: the
//! candidate/promoted/superseded/retired lifecycle with a revisioned head row plus an
//! append-only review audit table from `self_observations`, and the idempotent,
//! exact-ID-retrievable transactional style from `coordination`. See
//! `docs/Coordination-Phase-2-Design.md`.

use std::path::{Path, PathBuf};

use mempalace_core::WingId;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::types::RevisionedWrite;
use crate::{Result, StorageError};

const MAX_TEXT_BYTES: usize = 1024 * 1024;
const MAX_KEY_BYTES: usize = 256;

/// Lifecycle state for a versioned skill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillStatus {
    /// Proposed but not yet promoted to authoritative scope.
    Candidate,
    /// Reviewed and accepted as the current authoritative version for its scope.
    Promoted,
    /// Replaced by a newer promoted version of the same skill.
    Superseded,
    /// Reviewed and deliberately withdrawn.
    Retired,
}

impl SkillStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Candidate => "candidate",
            Self::Promoted => "promoted",
            Self::Superseded => "superseded",
            Self::Retired => "retired",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "candidate" => Ok(Self::Candidate),
            "promoted" => Ok(Self::Promoted),
            "superseded" => Ok(Self::Superseded),
            "retired" => Ok(Self::Retired),
            _ => Err(StorageError::Invariant(format!("unknown skill status `{value}`"))),
        }
    }
    fn terminal(self) -> bool {
        matches!(self, Self::Superseded | Self::Retired)
    }
}

/// Applicability boundary for a skill's authoritative scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillScope {
    /// Governed by, and self-promotable by, its own author only.
    Agent,
    /// Shared across a project; promotion requires recorded validation or a distinct reviewer.
    Project,
    /// Shared across an organization; promotion requires recorded validation or a distinct reviewer.
    Organization,
}

impl SkillScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Project => "project",
            Self::Organization => "organization",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "agent" => Ok(Self::Agent),
            "project" => Ok(Self::Project),
            "organization" => Ok(Self::Organization),
            _ => Err(StorageError::Invariant(format!("unknown skill scope `{value}`"))),
        }
    }
    fn requires_distinct_review(self) -> bool {
        matches!(self, Self::Project | Self::Organization)
    }
}

/// Outcome recorded against a specific skill version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillOutcomeResult {
    Success,
    Failure,
    Partial,
}

impl SkillOutcomeResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failure => "failure",
            Self::Partial => "partial",
        }
    }
    fn parse(value: &str) -> Result<Self> {
        match value {
            "success" => Ok(Self::Success),
            "failure" => Ok(Self::Failure),
            "partial" => Ok(Self::Partial),
            _ => Err(StorageError::Invariant(format!("unknown skill outcome result `{value}`"))),
        }
    }
}

/// Input for an idempotent skill proposal. A first proposal for a new `skill_id` becomes
/// version 1; proposing again for an existing `skill_id` becomes the next version and is
/// recorded as superseding the immediately prior version once promoted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewSkill {
    pub skill_id: String,
    pub scope: SkillScope,
    /// Owning project wing. Required for `project` scope, and rejected for the others, which
    /// are not project-bound.
    #[serde(default)]
    pub wing: Option<String>,
    pub applicability: String,
    pub instructions_ref: String,
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub required_tools: Vec<String>,
    #[serde(default)]
    pub required_permissions: Vec<String>,
    pub author: String,
    #[serde(default)]
    pub provenance: Option<Value>,
    pub confidence: f32,
    pub idempotency_key: String,
}

/// Authoritative, exact-ID-retrievable skill version record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Skill {
    pub skill_id: String,
    pub version: i64,
    pub scope: SkillScope,
    /// Owning project wing, present only for `project` scope.
    pub wing: Option<String>,
    pub status: SkillStatus,
    pub applicability: String,
    pub instructions_ref: String,
    pub required_capabilities: Vec<String>,
    pub required_tools: Vec<String>,
    pub required_permissions: Vec<String>,
    pub author: String,
    pub provenance: Option<Value>,
    pub confidence: f32,
    pub supersedes_version: Option<i64>,
    pub revision: i64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

/// Append-only lifecycle review/audit record for a skill version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillReview {
    pub review_id: String,
    pub skill_id: String,
    pub version: i64,
    pub from_status: SkillStatus,
    pub to_status: SkillStatus,
    pub reviewer: String,
    pub reason: String,
    #[serde(with = "time::serde::rfc3339")]
    pub occurred_at: OffsetDateTime,
}

/// Input for an idempotent outcome recording against a skill version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewSkillOutcome {
    pub skill_id: String,
    pub version: i64,
    #[serde(default)]
    pub task_id: Option<String>,
    pub result: SkillOutcomeResult,
    pub evaluator: String,
    #[serde(default)]
    pub notes: String,
    pub recorded_by: String,
    pub idempotency_key: String,
}

/// Append-only outcome record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkillOutcome {
    pub outcome_id: String,
    pub skill_id: String,
    pub version: i64,
    pub task_id: Option<String>,
    pub result: SkillOutcomeResult,
    pub evaluator: String,
    pub notes: String,
    pub recorded_by: String,
    #[serde(with = "time::serde::rfc3339")]
    pub recorded_at: OffsetDateTime,
}

/// SQLite-backed transactional skill registry.
#[derive(Debug, Clone)]
pub struct SkillStore {
    path: PathBuf,
}

impl SkillStore {
    /// Open skill registry state in the palace's operational SQLite database.
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self { path: path.as_ref().to_path_buf() }
    }

    /// Install skill registry tables and indexes.
    pub fn ensure_schema(&self) -> Result<()> {
        let conn = self.connection()?;
        conn.execute_batch(
            r#"
CREATE TABLE IF NOT EXISTS skills (
 skill_id TEXT NOT NULL, version INTEGER NOT NULL, scope TEXT NOT NULL, wing TEXT,
 status TEXT NOT NULL, applicability TEXT NOT NULL, instructions_ref TEXT NOT NULL, required_capabilities_json TEXT NOT NULL,
 required_tools_json TEXT NOT NULL, required_permissions_json TEXT NOT NULL, author TEXT NOT NULL,
 provenance_json TEXT, confidence REAL NOT NULL, supersedes_version INTEGER, revision INTEGER NOT NULL,
 idempotency_key TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
 PRIMARY KEY(skill_id, version), UNIQUE(author, idempotency_key));
CREATE INDEX IF NOT EXISTS idx_skills_scope_status ON skills(scope, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_skills_wing ON skills(wing, status, updated_at DESC);
CREATE TABLE IF NOT EXISTS skill_reviews (
 review_id TEXT PRIMARY KEY, skill_id TEXT NOT NULL, version INTEGER NOT NULL, from_status TEXT NOT NULL,
 to_status TEXT NOT NULL, reviewer TEXT NOT NULL, reason TEXT NOT NULL, occurred_at TEXT NOT NULL,
 FOREIGN KEY(skill_id, version) REFERENCES skills(skill_id, version));
CREATE INDEX IF NOT EXISTS idx_skill_reviews_skill ON skill_reviews(skill_id, version, occurred_at);
CREATE TABLE IF NOT EXISTS skill_outcomes (
 outcome_id TEXT PRIMARY KEY, skill_id TEXT NOT NULL, version INTEGER NOT NULL, task_id TEXT,
 result TEXT NOT NULL, evaluator TEXT NOT NULL, notes TEXT NOT NULL, recorded_by TEXT NOT NULL,
 idempotency_key TEXT NOT NULL, recorded_at TEXT NOT NULL, UNIQUE(recorded_by, idempotency_key),
 FOREIGN KEY(skill_id, version) REFERENCES skills(skill_id, version));
CREATE INDEX IF NOT EXISTS idx_skill_outcomes_skill ON skill_outcomes(skill_id, version, recorded_at);
"#,
        )?;
        backfill_wing_normalisation(&conn)?;
        Ok(())
    }

    /// Propose a skill, or return the prior committed version for an idempotency replay.
    ///
    /// The version is derived, never caller-supplied: it is one more than the highest existing
    /// version for `skill_id`, and `supersedes_version` is set automatically to the immediately
    /// prior version when one exists.
    pub fn propose_skill(&self, input: &NewSkill) -> Result<Skill> {
        validate_actor(&input.author)?;
        validate_key(&input.idempotency_key)?;
        bounded_text(&input.applicability, "skill applicability")?;
        bounded_text(&input.instructions_ref, "skill instructions_ref")?;
        if !(0.0..=1.0).contains(&input.confidence) {
            return Err(StorageError::Invariant("confidence must be within 0.0..=1.0".into()));
        }
        // `project` scope is meaningless without a project to scope to: without this, a
        // project-scoped skill would be authoritative in every wing of a shared palace.
        //
        // The wing is normalised here, on the way in, the same way task creation normalises it
        // in `coordination.rs`. Before this fix only the list path
        // (`SkillStore::list_skills`, called from the MCP layer via `parse_wing_id`) normalised;
        // propose stored the raw string, so proposing `myproject` and listing `myproject` could
        // silently never match a skill actually proposed under `wing_myproject`, or vice versa.
        let wing = match (input.scope, input.wing.as_deref().map(str::trim)) {
            (SkillScope::Project, None | Some("")) => {
                return Err(StorageError::Invariant(
                    "scope `project` requires a `wing` identifying the owning project".into(),
                ));
            }
            (SkillScope::Project, Some(wing)) => Some(WingId::normalized(wing)?.to_string()),
            (scope, Some(wing)) if !wing.is_empty() => {
                return Err(StorageError::Invariant(format!(
                    "scope `{}` is not project-bound and must not carry a wing (got `{wing}`)",
                    scope.as_str()
                )));
            }
            _ => None,
        };
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(skill) = find_skill_by_key(&tx, &input.author, &input.idempotency_key)? {
            tx.commit()?;
            return Ok(skill);
        }
        let max_version: Option<i64> = tx
            .query_row(
                "SELECT MAX(version) FROM skills WHERE skill_id=?1",
                [&input.skill_id],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let version = max_version.unwrap_or(0) + 1;
        let supersedes_version = max_version;
        // A skill belongs to one project for its whole life. Letting a later version move to a
        // different wing would be the same class of hole as a scope downgrade: a way to hand an
        // established skill_id to another project without review.
        if let Some(prior_version) = max_version {
            let prior = require_skill(&tx, &input.skill_id, prior_version)?;
            if prior.wing != wing {
                return Err(StorageError::Invariant(format!(
                    "skill `{}` is bound to wing {:?}; a new version cannot move it to {:?}",
                    input.skill_id, prior.wing, wing
                )));
            }
        }
        let now = OffsetDateTime::now_utc();
        tx.execute(
            "INSERT INTO skills(skill_id,version,scope,wing,status,applicability,instructions_ref,\
             required_capabilities_json,required_tools_json,required_permissions_json,author,\
             provenance_json,confidence,supersedes_version,revision,idempotency_key,created_at,updated_at)\
             VALUES(?1,?2,?3,?4,'candidate',?5,?6,?7,?8,?9,?10,?11,?12,?13,0,?14,?15,?15)",
            params![
                input.skill_id,
                version,
                input.scope.as_str(),
                wing,
                input.applicability,
                input.instructions_ref,
                serde_json::to_string(&input.required_capabilities)?,
                serde_json::to_string(&input.required_tools)?,
                serde_json::to_string(&input.required_permissions)?,
                input.author,
                input.provenance.as_ref().map(serde_json::to_string).transpose()?,
                input.confidence,
                supersedes_version,
                input.idempotency_key,
                format_time(now)?,
            ],
        )?;
        let skill = get_skill_tx(&tx, &input.skill_id, version)?
            .ok_or_else(|| StorageError::Invariant("proposed skill disappeared".into()))?;
        tx.commit()?;
        Ok(skill)
    }

    /// Retrieve a skill version by exact `(skill_id, version)`. `None` is an explicit
    /// authoritative miss; this never falls back to semantic search.
    pub fn get_skill(&self, skill_id: &str, version: i64) -> Result<Option<Skill>> {
        let conn = self.connection()?;
        get_skill_conn(&conn, skill_id, version)
    }

    /// List every version of a skill, newest first.
    pub fn list_skill_versions(&self, skill_id: &str) -> Result<Vec<Skill>> {
        let conn = self.connection()?;
        let mut statement =
            conn.prepare(&format!("{SKILL_COLUMNS_QUERY_PREFIX} ORDER BY version DESC"))?;
        statement.query_map(params![skill_id], skill_row)?.collect::<rusqlite::Result<Vec<_>>>().map_err(Into::into)
    }

    /// List skills for discovery, newest-updated first.
    ///
    /// Supplying `wing` excludes project-scoped skills owned by *other* wings, while keeping
    /// agent- and organization-scoped skills, which are not project-bound. Omitting `wing` spans
    /// every project and is an administrative view, not what a project-scoped agent should use.
    ///
    /// Discovery only: dereference a specific version with [`Self::get_skill`] before treating
    /// it as authoritative.
    pub fn list_skills(
        &self,
        scope: Option<SkillScope>,
        status: Option<SkillStatus>,
        wing: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Skill>> {
        let conn = self.connection()?;
        // Clamped like `CoordinationStore::events`, so an oversized caller limit cannot widen
        // the cast to a negative SQLite LIMIT (which means *no* limit) or load the registry.
        let limit = limit.clamp(1, 500) as i64;
        let mut predicates: Vec<String> = Vec::new();
        let mut bindings: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        if let Some(scope) = scope {
            bindings.push(Box::new(scope.as_str().to_owned()));
            predicates.push(format!("scope=?{}", bindings.len()));
        }
        if let Some(status) = status {
            bindings.push(Box::new(status.as_str().to_owned()));
            predicates.push(format!("status=?{}", bindings.len()));
        }
        if let Some(wing) = wing {
            bindings.push(Box::new(wing.to_owned()));
            predicates.push(format!("(wing IS NULL OR wing=?{})", bindings.len()));
        }
        bindings.push(Box::new(limit));
        let sql = format!(
            "{SKILL_LIST_QUERY_PREFIX}{}{} ORDER BY updated_at DESC LIMIT ?{}",
            if predicates.is_empty() { "" } else { " WHERE " },
            predicates.join(" AND "),
            bindings.len(),
        );
        let parameters =
            bindings.iter().map(AsRef::as_ref).collect::<Vec<&dyn rusqlite::ToSql>>();
        conn.prepare(&sql)?
            .query_map(parameters.as_slice(), skill_row)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Attach validation or field evidence to a skill version without changing its status.
    /// This is what a shared-scope promotion checks for before it is allowed to proceed.
    pub fn record_outcome(&self, input: &NewSkillOutcome) -> Result<SkillOutcome> {
        validate_actor(&input.recorded_by)?;
        validate_key(&input.idempotency_key)?;
        bounded_text(&input.notes, "skill outcome notes")?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(outcome) = find_outcome_by_key(&tx, &input.recorded_by, &input.idempotency_key)? {
            tx.commit()?;
            return Ok(outcome);
        }
        require_skill(&tx, &input.skill_id, input.version)?;
        let now = OffsetDateTime::now_utc();
        let id = format!("skill_outcome_{}", Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO skill_outcomes(outcome_id,skill_id,version,task_id,result,evaluator,notes,\
             recorded_by,idempotency_key,recorded_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                id,
                input.skill_id,
                input.version,
                input.task_id,
                input.result.as_str(),
                input.evaluator,
                input.notes,
                input.recorded_by,
                input.idempotency_key,
                format_time(now)?,
            ],
        )?;
        let outcome = get_outcome_tx(&tx, &id)?
            .ok_or_else(|| StorageError::Invariant("recorded outcome disappeared".into()))?;
        tx.commit()?;
        Ok(outcome)
    }

    /// Promote a candidate skill version to authoritative status for its scope.
    ///
    /// `agent`-scoped skills may be self-promoted by their author. `project`- and
    /// `organization`-scoped skills require at least one recorded [`SkillOutcome`] and a
    /// `reviewer` distinct from the skill's author — this is the enforced form of "candidate
    /// revisions cannot become shared authoritative procedures without configured validation or
    /// human approval." Promoting a new version atomically supersedes the immediately prior
    /// promoted version of the same `skill_id`, if one exists.
    pub fn promote_skill(
        &self,
        skill_id: &str,
        version: i64,
        expected_revision: i64,
        reviewer: &str,
        reason: &str,
    ) -> Result<RevisionedWrite<Skill>> {
        validate_actor(reviewer)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(current) = get_skill_tx(&tx, skill_id, version)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        if current.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(current.revision) });
        }
        if current.status.terminal() {
            return Err(StorageError::Invariant(format!(
                "skill `{skill_id}` version {version} is terminal and cannot be promoted"
            )));
        }
        if current.status == SkillStatus::Promoted {
            return Err(StorageError::Invariant(format!(
                "skill `{skill_id}` version {version} is already promoted"
            )));
        }
        // Whichever version is authoritative *right now* is the one this promotion displaces —
        // not merely whichever happened to be latest when this version was proposed. Reading it
        // before the promoting UPDATE means it can never be `current` itself.
        let superseded = find_promoted_version(&tx, skill_id)?;

        // Governance is the stricter of this version's own scope and the scope of the version it
        // would displace. Without that, proposing a weaker-scoped successor to a promoted
        // project- or organization-scoped skill would be a way to self-promote past shared review.
        let governing_scope = match &superseded {
            Some(prior) if prior.scope.requires_distinct_review() => prior.scope,
            _ => current.scope,
        };
        if governing_scope.requires_distinct_review() {
            if reviewer == current.author {
                return Err(StorageError::Invariant(format!(
                    "scope `{}` requires a reviewer distinct from the author",
                    governing_scope.as_str()
                )));
            }
            let has_validation: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM skill_outcomes WHERE skill_id=?1 AND version=?2)",
                params![skill_id, version],
                |r| r.get(0),
            )?;
            if !has_validation {
                return Err(StorageError::Invariant(format!(
                    "scope `{}` requires at least one recorded outcome before promotion",
                    governing_scope.as_str()
                )));
            }
        } else if reviewer != current.author {
            return Err(StorageError::Invariant(format!(
                "scope `agent` may only be promoted by its author `{}`",
                current.author
            )));
        }
        let now = OffsetDateTime::now_utc();
        let next_revision = current.revision + 1;
        let updated = tx.execute(
            "UPDATE skills SET status='promoted', revision=?3, updated_at=?4 \
             WHERE skill_id=?1 AND version=?2 AND revision=?5",
            params![skill_id, version, next_revision, format_time(now)?, expected_revision],
        )?;
        if updated != 1 {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        }
        insert_review(
            &tx,
            skill_id,
            version,
            current.status,
            SkillStatus::Promoted,
            reviewer,
            reason,
            now,
        )?;
        if let Some(prior) = superseded {
            supersede_promoted(&tx, &prior, reviewer, version, now)?;
        }
        let skill = get_skill_tx(&tx, skill_id, version)?
            .ok_or_else(|| StorageError::Invariant("promoted skill disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(skill))
    }

    /// Retire a non-terminal skill version.
    pub fn retire_skill(
        &self,
        skill_id: &str,
        version: i64,
        expected_revision: i64,
        reviewer: &str,
        reason: &str,
    ) -> Result<RevisionedWrite<Skill>> {
        validate_actor(reviewer)?;
        let mut conn = self.connection()?;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(current) = get_skill_tx(&tx, skill_id, version)? else {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        };
        if current.revision != expected_revision {
            return Ok(RevisionedWrite::Conflict { actual_revision: Some(current.revision) });
        }
        if current.status.terminal() {
            return Err(StorageError::Invariant(format!(
                "skill `{skill_id}` version {version} is already terminal"
            )));
        }
        let now = OffsetDateTime::now_utc();
        let next_revision = current.revision + 1;
        let updated = tx.execute(
            "UPDATE skills SET status='retired', revision=?3, updated_at=?4 \
             WHERE skill_id=?1 AND version=?2 AND revision=?5",
            params![skill_id, version, next_revision, format_time(now)?, expected_revision],
        )?;
        if updated != 1 {
            return Ok(RevisionedWrite::Conflict { actual_revision: None });
        }
        insert_review(&tx, skill_id, version, current.status, SkillStatus::Retired, reviewer, reason, now)?;
        let skill = get_skill_tx(&tx, skill_id, version)?
            .ok_or_else(|| StorageError::Invariant("retired skill disappeared".into()))?;
        tx.commit()?;
        Ok(RevisionedWrite::Applied(skill))
    }

    /// List the append-only review/audit trail for a skill version, oldest first.
    pub fn list_skill_reviews(&self, skill_id: &str, version: i64) -> Result<Vec<SkillReview>> {
        let conn = self.connection()?;
        let mut statement = conn.prepare(
            "SELECT review_id,skill_id,version,from_status,to_status,reviewer,reason,occurred_at \
             FROM skill_reviews WHERE skill_id=?1 AND version=?2 ORDER BY occurred_at",
        )?;
        statement
            .query_map(params![skill_id, version], review_row)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    fn connection(&self) -> Result<Connection> {
        let conn = Connection::open(&self.path)?;
        conn.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;",
        )?;
        Ok(conn)
    }
}

const SKILL_COLUMNS_QUERY_PREFIX: &str = "SELECT skill_id,version,scope,wing,status,applicability,instructions_ref,\
required_capabilities_json,required_tools_json,required_permissions_json,author,provenance_json,\
confidence,supersedes_version,revision,created_at,updated_at FROM skills WHERE skill_id=?1";
const SKILL_LIST_QUERY_PREFIX: &str = "SELECT skill_id,version,scope,wing,status,applicability,instructions_ref,\
required_capabilities_json,required_tools_json,required_permissions_json,author,provenance_json,\
confidence,supersedes_version,revision,created_at,updated_at FROM skills";

/// Return the single currently-promoted version of a skill, if one exists.
///
/// At most one version may be `promoted` at a time; more than one is a broken invariant rather
/// than a case to pick a winner from, so this reports it instead of silently choosing.
fn find_promoted_version(tx: &Transaction<'_>, skill_id: &str) -> Result<Option<Skill>> {
    let mut statement =
        tx.prepare(&format!("{SKILL_COLUMNS_QUERY_PREFIX} AND status='promoted' ORDER BY version"))?;
    let promoted =
        statement.query_map(params![skill_id], skill_row)?.collect::<rusqlite::Result<Vec<_>>>()?;
    match promoted.len() {
        0 => Ok(None),
        1 => Ok(promoted.into_iter().next()),
        count => Err(StorageError::Invariant(format!(
            "skill `{skill_id}` has {count} promoted versions; at most one may be authoritative"
        ))),
    }
}

fn supersede_promoted(
    tx: &Transaction<'_>,
    prior: &Skill,
    reviewer: &str,
    promoted_version: i64,
    now: OffsetDateTime,
) -> Result<()> {
    let updated = tx.execute(
        "UPDATE skills SET status='superseded', revision=?3, updated_at=?4 \
         WHERE skill_id=?1 AND version=?2 AND status='promoted' AND revision=?5",
        params![prior.skill_id, prior.version, prior.revision + 1, format_time(now)?, prior.revision],
    )?;
    if updated != 1 {
        return Err(StorageError::Invariant(format!(
            "skill `{}` version {} changed before it could be superseded",
            prior.skill_id, prior.version
        )));
    }
    insert_review(
        tx,
        &prior.skill_id,
        prior.version,
        SkillStatus::Promoted,
        SkillStatus::Superseded,
        reviewer,
        &format!("superseded by version {promoted_version}"),
        now,
    )
}

fn insert_review(
    tx: &Transaction<'_>,
    skill_id: &str,
    version: i64,
    from: SkillStatus,
    to: SkillStatus,
    reviewer: &str,
    reason: &str,
    at: OffsetDateTime,
) -> Result<()> {
    tx.execute(
        "INSERT INTO skill_reviews(review_id,skill_id,version,from_status,to_status,reviewer,reason,occurred_at)\
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        params![
            format!("skill_review_{}", Uuid::new_v4().simple()),
            skill_id,
            version,
            from.as_str(),
            to.as_str(),
            reviewer,
            reason,
            format_time(at)?,
        ],
    )?;
    Ok(())
}

fn require_skill(tx: &Transaction<'_>, skill_id: &str, version: i64) -> Result<Skill> {
    get_skill_tx(tx, skill_id, version)?
        .ok_or_else(|| StorageError::Invariant(format!("skill `{skill_id}` version {version} not found")))
}

fn get_skill_conn(conn: &Connection, skill_id: &str, version: i64) -> Result<Option<Skill>> {
    let mut statement = conn.prepare(&format!("{SKILL_COLUMNS_QUERY_PREFIX} AND version=?2"))?;
    statement.query_row(params![skill_id, version], skill_row).optional().map_err(Into::into)
}
fn get_skill_tx(tx: &Transaction<'_>, skill_id: &str, version: i64) -> Result<Option<Skill>> {
    get_skill_conn(tx, skill_id, version)
}
fn find_skill_by_key(tx: &Transaction<'_>, author: &str, key: &str) -> Result<Option<Skill>> {
    let id: Option<(String, i64)> = tx
        .query_row(
            "SELECT skill_id,version FROM skills WHERE author=?1 AND idempotency_key=?2",
            params![author, key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    id.map(|(skill_id, version)| require_skill(tx, &skill_id, version)).transpose()
}
fn skill_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Skill> {
    let scope: String = r.get(2)?;
    let status: String = r.get(4)?;
    let capabilities: String = r.get(7)?;
    let tools: String = r.get(8)?;
    let permissions: String = r.get(9)?;
    let provenance: Option<String> = r.get(11)?;
    let created: String = r.get(15)?;
    let updated: String = r.get(16)?;
    Ok(Skill {
        skill_id: r.get(0)?,
        version: r.get(1)?,
        scope: SkillScope::parse(&scope).map_err(sql_conv)?,
        wing: r.get(3)?,
        status: SkillStatus::parse(&status).map_err(sql_conv)?,
        applicability: r.get(5)?,
        instructions_ref: r.get(6)?,
        required_capabilities: serde_json::from_str(&capabilities).map_err(sql_conv)?,
        required_tools: serde_json::from_str(&tools).map_err(sql_conv)?,
        required_permissions: serde_json::from_str(&permissions).map_err(sql_conv)?,
        author: r.get(10)?,
        provenance: provenance.map(|v| serde_json::from_str(&v)).transpose().map_err(sql_conv)?,
        confidence: r.get(12)?,
        supersedes_version: r.get(13)?,
        revision: r.get(14)?,
        created_at: parse_time(created).map_err(sql_conv)?,
        updated_at: parse_time(updated).map_err(sql_conv)?,
    })
}

fn review_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<SkillReview> {
    let from: String = r.get(3)?;
    let to: String = r.get(4)?;
    let at: String = r.get(7)?;
    Ok(SkillReview {
        review_id: r.get(0)?,
        skill_id: r.get(1)?,
        version: r.get(2)?,
        from_status: SkillStatus::parse(&from).map_err(sql_conv)?,
        to_status: SkillStatus::parse(&to).map_err(sql_conv)?,
        reviewer: r.get(5)?,
        reason: r.get(6)?,
        occurred_at: parse_time(at).map_err(sql_conv)?,
    })
}

fn get_outcome_conn(conn: &Connection, id: &str) -> Result<Option<SkillOutcome>> {
    let mut statement = conn.prepare(
        "SELECT outcome_id,skill_id,version,task_id,result,evaluator,notes,recorded_by,recorded_at \
         FROM skill_outcomes WHERE outcome_id=?1",
    )?;
    statement.query_row([id], outcome_row).optional().map_err(Into::into)
}
fn get_outcome_tx(tx: &Transaction<'_>, id: &str) -> Result<Option<SkillOutcome>> {
    get_outcome_conn(tx, id)
}
fn find_outcome_by_key(tx: &Transaction<'_>, actor: &str, key: &str) -> Result<Option<SkillOutcome>> {
    let id: Option<String> = tx
        .query_row(
            "SELECT outcome_id FROM skill_outcomes WHERE recorded_by=?1 AND idempotency_key=?2",
            params![actor, key],
            |r| r.get(0),
        )
        .optional()?;
    id.map(|v| {
        get_outcome_tx(tx, &v)?
            .ok_or_else(|| StorageError::Invariant("outcome key points to missing row".into()))
    })
    .transpose()
}
fn outcome_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<SkillOutcome> {
    let result: String = r.get(4)?;
    let recorded: String = r.get(8)?;
    Ok(SkillOutcome {
        outcome_id: r.get(0)?,
        skill_id: r.get(1)?,
        version: r.get(2)?,
        task_id: r.get(3)?,
        result: SkillOutcomeResult::parse(&result).map_err(sql_conv)?,
        evaluator: r.get(5)?,
        notes: r.get(6)?,
        recorded_by: r.get(7)?,
        recorded_at: parse_time(recorded).map_err(sql_conv)?,
    })
}

/// Normalise any `wing` value stored before `propose_skill` started normalising it on the way
/// in (see the comment at that call site). Done in Rust, not SQL, because `WingId::normalized`
/// lowercases and substitutes invalid characters — a `'wing_' || wing` string concatenation
/// would not reproduce that. Idempotent: once every stored wing is already normalised this is a
/// cheap read with no writes, so it is safe to run on every `ensure_schema` call, matching
/// `CoordinationStore`'s upgrade-on-every-startup pattern. `wing` is not part of any unique
/// constraint on `skills`, so merging two raw spellings onto the same normalised value cannot
/// collide.
fn backfill_wing_normalisation(conn: &Connection) -> Result<()> {
    let mut statement = conn.prepare("SELECT DISTINCT wing FROM skills WHERE wing IS NOT NULL")?;
    let raw_wings = statement
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    for raw in raw_wings {
        let normalized = WingId::normalized(&raw)?.to_string();
        if normalized != raw {
            conn.execute("UPDATE skills SET wing=?1 WHERE wing=?2", params![normalized, raw])?;
        }
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
    if v.trim().is_empty() || v.len() > MAX_KEY_BYTES {
        Err(StorageError::Invariant("idempotency key must contain 1..=256 bytes".into()))
    } else {
        Ok(())
    }
}
fn bounded_text(value: &str, name: &str) -> Result<()> {
    if value.len() > MAX_TEXT_BYTES {
        Err(StorageError::Invariant(format!("{name} exceeds 1 MiB")))
    } else {
        Ok(())
    }
}
fn format_time(v: OffsetDateTime) -> Result<String> {
    v.format(&Rfc3339).map_err(|e| StorageError::Invariant(e.to_string()))
}
fn parse_time(v: String) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(&v, &Rfc3339).map_err(|e| StorageError::Invariant(e.to_string()))
}
fn sql_conv<E: std::error::Error + Send + Sync + 'static>(e: E) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store() -> (TempDir, SkillStore) {
        let dir = TempDir::new().expect("temp dir");
        let store = SkillStore::new(dir.path().join("storage.sqlite3"));
        store.ensure_schema().expect("schema");
        (dir, store)
    }

    fn agent_skill(s: &SkillStore, author: &str, key: &str) -> Skill {
        s.propose_skill(&NewSkill {
            skill_id: "coordinate-with-mempalace".into(),
            scope: SkillScope::Agent,
            wing: None,
            applicability: "when coordinating multiple workers".into(),
            instructions_ref: "skills/coordinate-with-mempalace/SKILL.md".into(),
            required_capabilities: vec!["coordination".into()],
            required_tools: vec!["mempalace_task_create".into()],
            required_permissions: vec![],
            author: author.into(),
            provenance: None,
            confidence: 0.8,
            idempotency_key: key.into(),
        })
        .expect("proposed skill")
    }

    #[test]
    fn propose_is_idempotent_and_versions_increment() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        assert_eq!(v1.version, 1);
        assert_eq!(v1.supersedes_version, None);
        let replay = agent_skill(&s, "author-a", "k1");
        assert_eq!(replay, v1);
        let v2 = agent_skill(&s, "author-a", "k2");
        assert_eq!(v2.version, 2);
        assert_eq!(v2.supersedes_version, Some(1));
    }

    #[test]
    fn exact_get_is_authoritative_and_never_falls_back() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        let fetched = s.get_skill(&v1.skill_id, v1.version).expect("get").expect("present");
        assert_eq!(fetched, v1);
        assert!(s.get_skill(&v1.skill_id, 999).expect("get").is_none());
        assert!(s.get_skill("no-such-skill", 1).expect("get").is_none());
    }

    #[test]
    fn agent_scope_self_promotes_without_validation() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        let promoted = s
            .promote_skill(&v1.skill_id, v1.version, v1.revision, "author-a", "looks good")
            .expect("promote");
        let RevisionedWrite::Applied(promoted) = promoted else { panic!("expected applied") };
        assert_eq!(promoted.status, SkillStatus::Promoted);
        assert_eq!(promoted.revision, v1.revision + 1);
    }

    #[test]
    fn shared_scope_requires_distinct_reviewer_and_recorded_validation() {
        let (_dir, s) = store();
        let v1 = s
            .propose_skill(&NewSkill {
                skill_id: "shared-skill".into(),
                scope: SkillScope::Project,
                wing: Some("wing_alpha".into()),
                applicability: "applies project-wide".into(),
                instructions_ref: "ref".into(),
                required_capabilities: vec![],
                required_tools: vec![],
                required_permissions: vec![],
                author: "author-a".into(),
                provenance: None,
                confidence: 0.5,
                idempotency_key: "shared-1".into(),
            })
            .expect("proposed skill");

        // Same author cannot self-promote a shared-scope skill.
        let err = s.promote_skill(&v1.skill_id, v1.version, v1.revision, "author-a", "self-approve");
        assert!(err.is_err());

        // Distinct reviewer without validation evidence is also rejected.
        let err = s.promote_skill(&v1.skill_id, v1.version, v1.revision, "reviewer-b", "no evidence yet");
        assert!(err.is_err());

        s.record_outcome(&NewSkillOutcome {
            skill_id: v1.skill_id.clone(),
            version: v1.version,
            task_id: None,
            result: SkillOutcomeResult::Success,
            evaluator: "eval-1".into(),
            notes: "passed integration test".into(),
            recorded_by: "reviewer-b".into(),
            idempotency_key: "outcome-1".into(),
        })
        .expect("outcome");

        let promoted = s
            .promote_skill(&v1.skill_id, v1.version, v1.revision, "reviewer-b", "validated")
            .expect("promote");
        assert!(matches!(promoted, RevisionedWrite::Applied(ref sk) if sk.status == SkillStatus::Promoted));
    }

    #[test]
    fn promoting_a_new_version_supersedes_the_prior_promoted_version() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        let RevisionedWrite::Applied(v1) =
            s.promote_skill(&v1.skill_id, v1.version, v1.revision, "author-a", "first").expect("promote")
        else {
            panic!("expected applied")
        };
        let v2 = agent_skill(&s, "author-a", "k2");
        let RevisionedWrite::Applied(v2) =
            s.promote_skill(&v2.skill_id, v2.version, v2.revision, "author-a", "second").expect("promote")
        else {
            panic!("expected applied")
        };
        assert_eq!(v2.status, SkillStatus::Promoted);
        let v1_after = s.get_skill(&v1.skill_id, v1.version).expect("get").expect("present");
        assert_eq!(v1_after.status, SkillStatus::Superseded);
    }

    #[test]
    fn stale_revision_is_a_conflict_not_a_silent_write() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        let result = s.promote_skill(&v1.skill_id, v1.version, v1.revision + 5, "author-a", "wrong revision");
        assert!(matches!(result, Ok(RevisionedWrite::Conflict { .. })));
        // The skill must remain untouched by the rejected write.
        let unchanged = s.get_skill(&v1.skill_id, v1.version).expect("get").expect("present");
        assert_eq!(unchanged.status, SkillStatus::Candidate);
        assert_eq!(unchanged.revision, v1.revision);
    }

    #[test]
    fn terminal_states_reject_further_transitions() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        let RevisionedWrite::Applied(retired) =
            s.retire_skill(&v1.skill_id, v1.version, v1.revision, "author-a", "no longer needed").expect("retire")
        else {
            panic!("expected applied")
        };
        assert_eq!(retired.status, SkillStatus::Retired);
        let err = s.promote_skill(&v1.skill_id, v1.version, retired.revision, "author-a", "too late");
        assert!(err.is_err());
    }

    #[test]
    fn outcomes_are_idempotent_and_scoped_to_a_specific_version() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        let outcome = NewSkillOutcome {
            skill_id: v1.skill_id.clone(),
            version: v1.version,
            task_id: Some("task_abc".into()),
            result: SkillOutcomeResult::Failure,
            evaluator: "eval-1".into(),
            notes: "regressed on edge case".into(),
            recorded_by: "worker-1".into(),
            idempotency_key: "outcome-dup".into(),
        };
        let first = s.record_outcome(&outcome).expect("first outcome");
        let replay = s.record_outcome(&outcome).expect("replayed outcome");
        assert_eq!(first, replay);
        // A distinct idempotency key against a nonexistent version is rejected, not replayed.
        assert!(
            s.record_outcome(&NewSkillOutcome {
                version: 999,
                idempotency_key: "outcome-nonexistent-version".into(),
                ..outcome
            })
            .is_err()
        );
    }

    #[test]
    fn review_trail_records_every_lifecycle_transition() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        s.promote_skill(&v1.skill_id, v1.version, v1.revision, "author-a", "go").expect("promote");
        let reviews = s.list_skill_reviews(&v1.skill_id, v1.version).expect("reviews");
        assert_eq!(reviews.len(), 1);
        assert_eq!(reviews[0].from_status, SkillStatus::Candidate);
        assert_eq!(reviews[0].to_status, SkillStatus::Promoted);
        assert_eq!(reviews[0].reviewer, "author-a");
    }

    #[test]
    fn out_of_order_promotion_leaves_only_one_promoted_version() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        s.promote_skill(&v1.skill_id, v1.version, v1.revision, "author-a", "first")
            .expect("promote v1");
        // v2 is proposed but deliberately left as a candidate.
        let _v2 = agent_skill(&s, "author-a", "k2");
        let v3 = agent_skill(&s, "author-a", "k3");
        s.promote_skill(&v3.skill_id, v3.version, v3.revision, "author-a", "third")
            .expect("promote v3");

        let promoted = s
            .list_skill_versions(&v1.skill_id)
            .expect("versions")
            .into_iter()
            .filter(|skill| skill.status == SkillStatus::Promoted)
            .map(|skill| skill.version)
            .collect::<Vec<_>>();
        assert_eq!(promoted, vec![3], "exactly one version may be authoritative");
    }

    #[test]
    fn agent_scope_promotion_is_restricted_to_the_author() {
        let (_dir, s) = store();
        let v1 = agent_skill(&s, "author-a", "k1");
        let hijack =
            s.promote_skill(&v1.skill_id, v1.version, v1.revision, "someone-else", "not mine");
        assert!(hijack.is_err(), "only the author may promote an agent-scoped skill");
    }

    #[test]
    fn scope_cannot_be_downgraded_to_escape_shared_governance() {
        let (_dir, s) = store();
        let shared = s
            .propose_skill(&NewSkill {
                skill_id: "governed".into(),
                scope: SkillScope::Project,
                wing: Some("wing_alpha".into()),
                applicability: "project wide".into(),
                instructions_ref: "ref".into(),
                required_capabilities: vec![],
                required_tools: vec![],
                required_permissions: vec![],
                author: "author-a".into(),
                provenance: None,
                confidence: 0.5,
                idempotency_key: "governed-1".into(),
            })
            .expect("v1");
        s.record_outcome(&NewSkillOutcome {
            skill_id: shared.skill_id.clone(),
            version: shared.version,
            task_id: None,
            result: SkillOutcomeResult::Success,
            evaluator: "eval".into(),
            notes: String::new(),
            recorded_by: "reviewer-b".into(),
            idempotency_key: "governed-outcome".into(),
        })
        .expect("outcome");
        s.promote_skill(&shared.skill_id, shared.version, shared.revision, "reviewer-b", "ok")
            .expect("promote v1");

        // Proposing v2 at a weaker scope must not become a way to bypass v1's governance.
        let downgraded = s.propose_skill(&NewSkill {
            skill_id: "governed".into(),
            scope: SkillScope::Agent,
            wing: None,
            applicability: "sneaky".into(),
            instructions_ref: "ref".into(),
            required_capabilities: vec![],
            required_tools: vec![],
            required_permissions: vec![],
            author: "author-a".into(),
            provenance: None,
            confidence: 0.9,
            idempotency_key: "governed-2".into(),
        });
        let Ok(downgraded) = downgraded else {
            return; // Rejecting the proposal outright is an acceptable resolution.
        };
        let promoted = s.promote_skill(
            &downgraded.skill_id,
            downgraded.version,
            downgraded.revision,
            "author-a",
            "self approve at agent scope",
        );
        assert!(
            promoted.is_err(),
            "a scope downgrade must not let an author supersede a governed shared version"
        );
    }


    #[test]
    fn project_scope_requires_a_wing_and_others_reject_one() {
        let (_dir, s) = store();
        let missing = s.propose_skill(&NewSkill {
            skill_id: "needs-wing".into(),
            scope: SkillScope::Project,
            wing: None,
            applicability: "a".into(),
            instructions_ref: "r".into(),
            required_capabilities: vec![],
            required_tools: vec![],
            required_permissions: vec![],
            author: "author-a".into(),
            provenance: None,
            confidence: 0.5,
            idempotency_key: "k1".into(),
        });
        assert!(missing.is_err(), "project scope without a wing is meaningless");

        let stray = s.propose_skill(&NewSkill {
            skill_id: "not-project".into(),
            scope: SkillScope::Organization,
            wing: Some("wing_alpha".into()),
            applicability: "a".into(),
            instructions_ref: "r".into(),
            required_capabilities: vec![],
            required_tools: vec![],
            required_permissions: vec![],
            author: "author-a".into(),
            provenance: None,
            confidence: 0.5,
            idempotency_key: "k2".into(),
        });
        assert!(stray.is_err(), "a non-project scope must not carry a wing");
    }

    #[test]
    fn propose_normalises_the_wing_the_same_way_list_does() {
        let (_dir, s) = store();
        let proposed = s
            .propose_skill(&NewSkill {
                skill_id: "unprefixed-wing-skill".into(),
                scope: SkillScope::Project,
                wing: Some("myproject".into()),
                applicability: "a".into(),
                instructions_ref: "r".into(),
                required_capabilities: vec![],
                required_tools: vec![],
                required_permissions: vec![],
                author: "author-a".into(),
                provenance: None,
                confidence: 0.5,
                idempotency_key: "unprefixed-1".into(),
            })
            .expect("proposed with an unprefixed wing");
        assert_eq!(
            proposed.wing.as_deref(),
            Some("wing_myproject"),
            "propose must normalise the wing the same way task creation does"
        );

        // Regression: `list_skills` is always called with an already-normalised wing from the
        // MCP layer (`parse_wing_id`); proposing and listing with the same unprefixed wing must
        // agree once propose also normalises, matching what a caller who typed `myproject` both
        // times would expect.
        let seen = s
            .list_skills(None, None, Some("wing_myproject"), 50)
            .expect("list with the normalised spelling");
        assert!(
            seen.iter().any(|skill| skill.skill_id == "unprefixed-wing-skill"),
            "a skill proposed with an unprefixed wing must be found once the filter is normalised the same way"
        );
    }

    #[test]
    fn ensure_schema_backfills_wings_stored_before_propose_normalised_them() {
        let (dir, s) = store();
        let path = dir.path().join("storage.sqlite3");
        // Insert directly, bypassing `propose_skill`, to simulate a row written before this fix
        // landed — exactly the shape `propose_skill` used to produce for a raw `"myproject"`.
        {
            let conn = Connection::open(&path).expect("open");
            conn.execute(
                "INSERT INTO skills(skill_id,version,scope,wing,status,applicability,instructions_ref,\
                 required_capabilities_json,required_tools_json,required_permissions_json,author,\
                 provenance_json,confidence,supersedes_version,revision,idempotency_key,created_at,updated_at)\
                 VALUES('legacy-skill',1,'project','myproject','candidate','a','r','[]','[]','[]',\
                 'author-a',NULL,0.5,NULL,0,'legacy-key','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
                [],
            )
            .expect("legacy row with an unnormalised wing");
        }

        // `ensure_schema` runs on every `McpRuntime::new`, so re-running it (not just the first
        // call the `store()` helper already made) is the realistic shape of the guarantee.
        s.ensure_schema().expect("backfill pass");

        let fetched = s.get_skill("legacy-skill", 1).expect("get").expect("found");
        assert_eq!(
            fetched.wing.as_deref(),
            Some("wing_myproject"),
            "a pre-existing raw wing must be normalised by the backfill"
        );
        let seen = s.list_skills(None, None, Some("wing_myproject"), 50).expect("list");
        assert!(
            seen.iter().any(|skill| skill.skill_id == "legacy-skill"),
            "the backfilled skill must now be discoverable under the normalised wing"
        );

        // Idempotence: running the backfill again over already-normalised data must not error
        // or change anything further.
        s.ensure_schema().expect("second backfill pass is a no-op");
        assert_eq!(
            s.get_skill("legacy-skill", 1).expect("get").expect("found").wing.as_deref(),
            Some("wing_myproject")
        );
    }

    #[test]
    fn discovery_hides_project_skills_owned_by_other_wings() {
        let (_dir, s) = store();
        let project_skill = |id: &str, wing: &str, key: &str| NewSkill {
            skill_id: id.into(),
            scope: SkillScope::Project,
            wing: Some(wing.into()),
            applicability: "a".into(),
            instructions_ref: "r".into(),
            required_capabilities: vec![],
            required_tools: vec![],
            required_permissions: vec![],
            author: "author-a".into(),
            provenance: None,
            confidence: 0.5,
            idempotency_key: key.into(),
        };
        s.propose_skill(&project_skill("alpha-deploy", "wing_alpha", "k1")).expect("alpha");
        s.propose_skill(&project_skill("beta-deploy", "wing_beta", "k2")).expect("beta");
        s.propose_skill(&NewSkill {
            skill_id: "palace-wide".into(),
            scope: SkillScope::Organization,
            wing: None,
            applicability: "a".into(),
            instructions_ref: "r".into(),
            required_capabilities: vec![],
            required_tools: vec![],
            required_permissions: vec![],
            author: "author-a".into(),
            provenance: None,
            confidence: 0.5,
            idempotency_key: "k3".into(),
        })
        .expect("org");

        let seen = s.list_skills(None, None, Some("wing_alpha"), 50).expect("list");
        let ids = seen.iter().map(|sk| sk.skill_id.as_str()).collect::<Vec<_>>();
        assert!(ids.contains(&"alpha-deploy"), "own project skill must be visible");
        assert!(ids.contains(&"palace-wide"), "non-project-bound skills stay visible");
        assert!(!ids.contains(&"beta-deploy"), "another project's skill must not appear");
    }

    #[test]
    fn a_later_version_cannot_move_a_skill_to_another_wing() {
        let (_dir, s) = store();
        let base = |wing: &str, key: &str| NewSkill {
            skill_id: "bound".into(),
            scope: SkillScope::Project,
            wing: Some(wing.into()),
            applicability: "a".into(),
            instructions_ref: "r".into(),
            required_capabilities: vec![],
            required_tools: vec![],
            required_permissions: vec![],
            author: "author-a".into(),
            provenance: None,
            confidence: 0.5,
            idempotency_key: key.into(),
        };
        s.propose_skill(&base("wing_alpha", "k1")).expect("v1");
        let moved = s.propose_skill(&base("wing_beta", "k2"));
        assert!(moved.is_err(), "a skill stays bound to one project across its versions");
    }

    #[test]
    fn list_skills_filters_by_scope_and_status() {
        let (_dir, s) = store();
        let agent_v1 = agent_skill(&s, "author-a", "k1");
        s.promote_skill(&agent_v1.skill_id, agent_v1.version, agent_v1.revision, "author-a", "go").expect("promote");
        s.propose_skill(&NewSkill {
            skill_id: "other-shared".into(),
            scope: SkillScope::Organization,
            wing: None,
            applicability: "org wide".into(),
            instructions_ref: "ref".into(),
            required_capabilities: vec![],
            required_tools: vec![],
            required_permissions: vec![],
            author: "author-b".into(),
            provenance: None,
            confidence: 0.3,
            idempotency_key: "org-1".into(),
        })
        .expect("proposed skill");

        let promoted_agent = s.list_skills(Some(SkillScope::Agent), Some(SkillStatus::Promoted), None, 10).expect("list");
        assert_eq!(promoted_agent.len(), 1);
        assert_eq!(promoted_agent[0].skill_id, "coordinate-with-mempalace");

        let candidates = s.list_skills(None, Some(SkillStatus::Candidate), None, 10).expect("list");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].skill_id, "other-shared");
    }
}
