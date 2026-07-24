use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime};

use crate::locator::SourceLocator;
use crate::{DrawerId, EmbeddingProfile, RoomId, WingId};

time::serde::format_description!(date_only, Date, "[year]-[month]-[day]");

/// Durable repository-view metadata for project-mined rows.
///
/// Replaces the `hall = "view:<name>"` convention with explicit, queryable
/// columns.  `None` on a `DrawerRecord` means the row is not project-mined
/// (e.g. a conversation, diary entry, or authored drawer).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryViewMetadata {
    /// Stable repository identity (normalised git remote URL, or `wing:<wing>`).
    pub repo_id: String,
    /// View name: `None` for canonical (default-branch) snapshots, `Some(...)` for
    /// a named branch or detached-HEAD view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_name: Option<String>,
    /// Absolute project checkout path on the palace host that owns this row.
    pub source_path: String,
    /// Git commit hash at mine time (`git rev-parse HEAD`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_commit: Option<String>,
    /// Base/integration ref (the default branch name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    /// Merge-base commit between the view and the base ref.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_base: Option<String>,
    /// Stable worktree identity (canonicalised checkout path hash).
    pub worktree_id: String,
    /// Path state: `"present"` or `"deleted"`.  `"deleted"` marks a tombstone row
    /// whose source file has been removed from the checkout.
    pub path_state: String,
}

/// Canonical drawer row shape for future storage adapters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DrawerRecord {
    pub id: DrawerId,
    pub wing: WingId,
    pub room: RoomId,
    pub hall: Option<String>,
    #[serde(with = "date_only::option", default)]
    pub date: Option<Date>,
    pub source_file: String,
    pub chunk_index: u32,
    pub ingest_mode: String,
    pub extract_mode: Option<String>,
    pub added_by: String,
    #[serde(with = "time::serde::rfc3339")]
    pub filed_at: OffsetDateTime,
    pub importance: Option<f32>,
    pub emotional_weight: Option<f32>,
    pub weight: Option<f32>,
    pub content: String,
    pub content_hash: String,
    #[serde(default)]
    pub embedding: Vec<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator: Option<SourceLocator>,
    /// Repository-view metadata for project-mined rows.  Absent for non-project
    /// rows and for legacy rows that predate this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view_metadata: Option<RepositoryViewMetadata>,
}

/// Search request contract shared by CLI, MCP, and library APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchQuery {
    pub text: String,
    pub wing: Option<WingId>,
    pub room: Option<RoomId>,
    /// Optional view/ref name to scope search to a specific branch view.
    /// When `None`, search includes canonical snapshots and all branch views
    /// (legacy behaviour).  Set to `"canonical"` to restrict to canonical
    /// snapshots, or to a specific branch name for a single branch view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
    pub limit: usize,
    pub profile: EmbeddingProfile,
}

/// Search result contract shared by CLI, MCP, and library APIs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drawer_id: Option<DrawerId>,
    pub wing: WingId,
    pub room: RoomId,
    #[serde(rename = "similarity")]
    pub score: f32,
    #[serde(rename = "text")]
    pub content: String,
    pub source_file: String,
    /// `true` when the result comes from a locator-backed row whose source file
    /// changed since mining.  Absent (serialised) unless true, so existing JSON
    /// shapes remain byte-identical for non-stale results.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stale: bool,
    /// Content hash of the drawer, present only when the result is from a
    /// duplicate check that needs exact-match detection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    /// View/ref name this result belongs to, if mined from a specific branch view.
    /// Absent for canonical results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub view: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;
    use time::macros::{date, datetime};

    use super::{DrawerRecord, SearchResult};
    use crate::{DrawerId, RoomId, WingId};

    #[test]
    fn search_result_uses_phase0_field_names() {
        let result = SearchResult {
            drawer_id: None,
            wing: WingId::new("project_alpha").unwrap(),
            room: RoomId::new("backend").unwrap(),
            score: 0.49,
            content: "auth migration parity".to_owned(),
            source_file: "team.txt".to_owned(),
            stale: false,
            content_hash: None,
            view: None,
        };

        let value = serde_json::to_value(&result).unwrap();
        let object = value.as_object().unwrap();

        assert_eq!(object.get("wing"), Some(&json!("project_alpha")));
        assert_eq!(object.get("room"), Some(&json!("backend")));
        assert_eq!(object.get("text"), Some(&json!("auth migration parity")));
        assert_eq!(object.get("source_file"), Some(&json!("team.txt")));
        assert!(object.get("drawer_id").is_none());
        let similarity = object.get("similarity").and_then(|value| value.as_f64()).unwrap();
        assert!((similarity - 0.49).abs() < 1e-6, "unexpected similarity: {similarity}");
        // `stale` must be absent when false so byte-parity is preserved.
        assert!(object.get("stale").is_none(), "stale should be absent when false");
        // `view` must be absent when None so byte-parity is preserved for non-view results.
        assert!(object.get("view").is_none(), "view should be absent when None");
    }

    #[test]
    fn search_result_stale_present_only_when_true() {
        let non_stale = SearchResult {
            drawer_id: None,
            wing: WingId::new("w").unwrap(),
            room: RoomId::new("r").unwrap(),
            score: 0.5,
            content: "text".to_owned(),
            source_file: "f.txt".to_owned(),
            stale: false,
            content_hash: None,
            view: None,
        };
        let stale = SearchResult { stale: true, ..non_stale.clone() };
        let non_stale_json = serde_json::to_value(&non_stale).unwrap();
        let stale_json = serde_json::to_value(&stale).unwrap();
        assert!(non_stale_json.as_object().unwrap().get("stale").is_none());
        assert_eq!(stale_json.as_object().unwrap().get("stale"), Some(&json!(true)));
    }

    #[test]
    fn drawer_record_serializes_as_json_strings_for_time_fields() {
        let record = DrawerRecord {
            id: DrawerId::new("project_alpha/backend/0001").unwrap(),
            wing: WingId::new("project_alpha").unwrap(),
            room: RoomId::new("backend").unwrap(),
            hall: Some("auth".to_owned()),
            date: Some(date!(2026 - 04 - 11)),
            source_file: "auth.py".to_owned(),
            chunk_index: 0,
            ingest_mode: "phase0".to_owned(),
            extract_mode: Some("full".to_owned()),
            added_by: "tester".to_owned(),
            filed_at: datetime!(2026-04-11 09:45:00 UTC),
            importance: Some(0.8),
            emotional_weight: Some(0.2),
            weight: Some(1.0),
            content: "payload".to_owned(),
            content_hash: "hash".to_owned(),
            embedding: vec![0.1, 0.2],
            locator: None,
            view_metadata: None,
        };

        let value = serde_json::to_value(&record).unwrap();

        assert_eq!(value.get("date"), Some(&json!("2026-04-11")));
        assert_eq!(value.get("filed_at"), Some(&json!("2026-04-11T09:45:00Z")));

        let embedding = value.get("embedding").and_then(|value| value.as_array()).unwrap();
        let first = embedding[0].as_f64().unwrap();
        let second = embedding[1].as_f64().unwrap();
        assert!((first - 0.1).abs() < 1e-6, "unexpected embedding[0]: {first}");
        assert!((second - 0.2).abs() < 1e-6, "unexpected embedding[1]: {second}");
    }
}
