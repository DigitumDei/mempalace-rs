use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime};

use crate::{DrawerId, EmbeddingProfile, RoomId, WingId};

time::serde::format_description!(date_only, Date, "[year]-[month]-[day]");

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
}

/// Search request contract shared by CLI, MCP, and library APIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchQuery {
    pub text: String,
    pub wing: Option<WingId>,
    pub room: Option<RoomId>,
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
