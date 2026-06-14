//! Wire-types (serde DTOs) shared between the MemPalace federation HTTP server
//! and any federation client. This crate is deliberately free of runtime logic
//! so it can be compiled into both the server and lightweight clients.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The federation HTTP API version implemented by this crate.
pub const FEDERATION_API_VERSION: u32 = 1;

// ─── Server info ──────────────────────────────────────────────────────────────

/// Response body for the `GET /info` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InfoResponse {
    /// Semver string of the running MemPalace server binary.
    pub server_version: String,
    /// Federation API version (matches [`FEDERATION_API_VERSION`]).
    pub federation_api_version: u32,
    /// Name of the embedding profile used by this server (e.g. `"balanced"`).
    pub embedding_profile: String,
    /// Feature flags the server supports (e.g. `["drawers", "kg", "changes"]`).
    pub capabilities: Vec<String>,
}

// ─── Drawer search ────────────────────────────────────────────────────────────

/// Request body for `POST /drawers/search`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DrawerSearchRequest {
    /// Free-text semantic query.
    pub query: String,
    /// Optional wing filter.
    #[serde(default)]
    pub wing: Option<String>,
    /// Optional room filter.
    #[serde(default)]
    pub room: Option<String>,
    /// Maximum number of results to return.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// Response body for `POST /drawers/search`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DrawerSearchResponse {
    /// Ranked list of matching drawers.
    pub results: Vec<RemoteDrawerResult>,
}

/// A single drawer hit returned by the federation search endpoint.
///
/// `rank` is mandatory because similarity scores are not comparable across
/// embedding profiles; clients that merge results from multiple servers must
/// merge by rank rather than by raw score.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RemoteDrawerResult {
    /// Stable drawer identifier.
    pub drawer_id: String,
    /// Wing the drawer lives in.
    pub wing: String,
    /// Room the drawer lives in.
    pub room: String,
    /// 1-based rank within this server's result set.
    pub rank: usize,
    /// Normalised similarity score in `[0, 1]`.
    pub score: f32,
    /// Full drawer content text.
    pub content: String,
    /// Original source file path, if recorded.
    #[serde(default)]
    pub source_file: Option<String>,
    /// BLAKE3 hex hash of the content at ingest time.
    #[serde(default)]
    pub content_hash: Option<String>,
    /// RFC 3339 timestamp when the drawer was filed.
    #[serde(default)]
    pub filed_at: Option<String>,
    /// Agent name recorded at ingest time.
    #[serde(default)]
    pub added_by: Option<String>,
    /// True when the drawer's mined source file changed since mining
    /// (locator-backed rows).  Absent unless true; `serde(default)` keeps old
    /// servers/clients wire-compatible.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stale: bool,
}

// ─── Add drawer ───────────────────────────────────────────────────────────────

/// Request body for `POST /drawers`.
///
/// The server will override or augment `added_by` from the auth token; the
/// field here carries the client's claimed agent name.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AddDrawerRequest {
    /// Target wing.
    pub wing: String,
    /// Target room.
    pub room: String,
    /// Verbatim content to store.
    pub content: String,
    /// Optional source file path.
    #[serde(default)]
    pub source_file: Option<String>,
    /// Client-declared agent name.
    #[serde(default)]
    pub added_by: Option<String>,
}

/// Response body for `POST /drawers`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AddDrawerResponse {
    /// Whether the drawer was successfully stored.
    pub success: bool,
    /// Assigned drawer identifier, present when `success` is `true`.
    #[serde(default)]
    pub drawer_id: Option<String>,
    /// Resolved wing, present when `success` is `true`.
    #[serde(default)]
    pub wing: Option<String>,
    /// Resolved room, present when `success` is `true`.
    #[serde(default)]
    pub room: Option<String>,
}

// ─── Duplicate check ──────────────────────────────────────────────────────────

/// Request body for `POST /drawers/check-duplicate`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckDuplicateRequest {
    /// Content to test for near-duplicates.
    pub content: String,
    /// Similarity threshold in `[0, 1]`; defaults to server's configured value.
    #[serde(default)]
    pub threshold: Option<f32>,
}

/// Response body for `POST /drawers/check-duplicate`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckDuplicateResponse {
    /// `true` when at least one near-duplicate was found above the threshold.
    pub is_duplicate: bool,
    /// Matching drawers as a JSON array (mirrors the MCP tool payload shape).
    pub matches: Value,
}

// ─── List drawers ─────────────────────────────────────────────────────────────

/// Query parameters for `GET /drawers`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ListDrawersQuery {
    /// Optional wing filter.
    #[serde(default)]
    pub wing: Option<String>,
    /// Optional room filter.
    #[serde(default)]
    pub room: Option<String>,
    /// Maximum number of results per page.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Opaque pagination cursor returned by a previous response.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Response body for `GET /drawers`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ListDrawersResponse {
    /// Array of drawer objects (shape mirrors the storage layer).
    pub drawers: Value,
    /// Opaque cursor to pass back for the next page; `null` when exhausted.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

// ─── Knowledge graph ──────────────────────────────────────────────────────────

/// Request body for `POST /kg/query`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KgQueryRequest {
    /// Entity name to query.
    pub entity: String,
    /// Optional RFC 3339 or `YYYY-MM-DD` date for point-in-time queries.
    #[serde(default)]
    pub as_of: Option<String>,
    /// Traversal direction: `"outgoing"`, `"incoming"`, or `"both"`.
    #[serde(default)]
    pub direction: Option<String>,
}

/// Request body for `POST /kg/facts`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KgAddFactRequest {
    /// Subject entity.
    pub subject: String,
    /// Predicate / relationship type.
    pub predicate: String,
    /// Object entity.
    pub object: String,
    /// Optional `YYYY-MM-DD` date when the fact became true.
    #[serde(default)]
    pub valid_from: Option<String>,
}

/// Request body for `POST /kg/invalidate`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KgInvalidateRequest {
    /// Subject entity.
    pub subject: String,
    /// Predicate / relationship type.
    pub predicate: String,
    /// Object entity.
    pub object: String,
    /// Optional `YYYY-MM-DD` date when the fact stopped being true.
    #[serde(default)]
    pub ended: Option<String>,
}

// KG responses are plain `serde_json::Value` pass-throughs (server mirrors MCP
// tool payloads); no response DTOs are needed.

// ─── Change log ───────────────────────────────────────────────────────────────

/// Query parameters for `GET /changes`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangesQuery {
    /// Return only events at or after this RFC 3339 timestamp.
    #[serde(default)]
    pub since: Option<String>,
    /// Maximum number of events per page.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Opaque pagination cursor returned by a previous response.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Response body for `GET /changes`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangesResponse {
    /// Ordered list of change events.
    pub events: Vec<ChangeEventDto>,
    /// Opaque cursor to pass back for the next page; `null` when exhausted.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// A single change event in the federation changes feed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChangeEventDto {
    /// Operation type (e.g. `"drawer_added"`, `"kg_fact_added"`).
    pub event_type: String,
    /// RFC 3339 timestamp when the event occurred.
    pub occurred_at: String,
    /// Primary identifier of the affected entity.
    pub entity_id: String,
    /// Agent or tool that performed the write, if known.
    #[serde(default)]
    pub actor: Option<String>,
    /// Optional JSON payload with extra context.
    #[serde(default)]
    pub details: Option<Value>,
}

// ─── Bulk ingest ──────────────────────────────────────────────────────────────

/// Request body for `POST /v1/ingest/batch`.
///
/// The client sends one or more pre-chunked files; the server embeds them and
/// writes drawers on its side.  `wing` and `repo_id` together determine the
/// source-key namespace so that two clients pushing the same repository
/// converge on identical drawer ids.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestBatchRequest {
    /// Target wing name (will be normalized server-side).
    pub wing: String,
    /// Machine-independent repository identity (normalized remote-URL or fallback).
    pub repo_id: String,
    /// Client-declared agent name; server may augment from auth token.
    #[serde(default)]
    pub agent: Option<String>,
    /// Git commit hash at the time of mining, for audit / change-event details.
    #[serde(default)]
    pub commit_hash: Option<String>,
    /// Files included in this batch.
    pub files: Vec<IngestFileDto>,
}

/// A single file's chunks within an [`IngestBatchRequest`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestFileDto {
    /// Repository-root-relative path with forward slashes.
    pub relative_path: String,
    /// `project_ingest_content_hash` of this file; used for skip-unchanged detection.
    pub content_hash: String,
    /// BLAKE3 hex hash of the full file bytes.  When `Some`, all chunks carry
    /// byte ranges so the server can build locator rows.  When `None`, the
    /// file could not be read as UTF-8 and chunks are stored as legacy content
    /// rows (text persisted, no locator).
    #[serde(default)]
    pub file_hash: Option<String>,
    /// Ordered list of chunks derived from this file.
    pub chunks: Vec<IngestChunkDto>,
}

/// A single text chunk within an [`IngestFileDto`].
///
/// When `file_hash` is `Some` on the parent [`IngestFileDto`], all four byte/
/// line range fields must also be `Some` so the server can store a locator row.
/// When `file_hash` is `None`, the ranges are absent and the server stores
/// `text` directly as a legacy content row.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestChunkDto {
    /// 0-based chunk index within the file; determines the drawer-id suffix.
    pub chunk_index: u32,
    /// Room name for this chunk (derived from file path / heuristics on the client).
    pub room: String,
    /// Chunk text; the server uses this for embedding.
    pub text: String,
    /// Inclusive byte offset of the first byte of this chunk in the file.
    #[serde(default)]
    pub byte_start: Option<u64>,
    /// Byte offset one past the last byte of this chunk (exclusive).
    #[serde(default)]
    pub byte_end: Option<u64>,
    /// 1-based line number of the first line of this chunk.
    #[serde(default)]
    pub line_start: Option<u32>,
    /// 1-based line number of the last line of this chunk.
    #[serde(default)]
    pub line_end: Option<u32>,
}

/// Response body for `POST /v1/ingest/batch`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestBatchResponse {
    /// Per-file outcomes, in the same order as the request's `files` array.
    pub files: Vec<IngestFileResult>,
    /// Non-fatal warnings (e.g. missing checkout mapping → stale placeholders).
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// Outcome for a single file within an [`IngestBatchResponse`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IngestFileResult {
    /// Repository-root-relative path, echoed from the request.
    pub relative_path: String,
    /// `"ingested"` | `"skipped_unchanged"` | `"failed"`.
    pub status: String,
    /// Number of drawers written (0 for skipped or failed files).
    #[serde(default)]
    pub drawers_written: usize,
    /// Error message when `status == "failed"`.
    #[serde(default)]
    pub error: Option<String>,
}

// ─── Error ────────────────────────────────────────────────────────────────────

/// Standard error response body returned by all federation endpoints on failure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorBody {
    /// Machine-readable error code (e.g. `"not_found"`, `"invalid_params"`).
    pub code: String,
    /// Human-readable error message.
    pub message: String,
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn info_response_round_trips() {
        let original = InfoResponse {
            server_version: "2.0.0".to_owned(),
            federation_api_version: FEDERATION_API_VERSION,
            embedding_profile: "balanced".to_owned(),
            capabilities: vec!["drawers".to_owned(), "kg".to_owned()],
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: InfoResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn drawer_search_request_sparse_json_deserialises() {
        // Only `query` is required; optional fields should default to None.
        let raw = r#"{"query":"hello"}"#;
        let req: DrawerSearchRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.query, "hello");
        assert!(req.wing.is_none());
        assert!(req.room.is_none());
        assert!(req.limit.is_none());
    }

    #[test]
    fn drawer_search_response_round_trips() {
        let original = DrawerSearchResponse {
            results: vec![RemoteDrawerResult {
                drawer_id: "drw_1".to_owned(),
                wing: "wing_code".to_owned(),
                room: "backend".to_owned(),
                rank: 1,
                score: 0.95,
                content: "hello world".to_owned(),
                source_file: Some("src/main.rs".to_owned()),
                content_hash: None,
                filed_at: None,
                added_by: None,
                stale: false,
            }],
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: DrawerSearchResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn add_drawer_request_sparse_json_deserialises() {
        let raw = r#"{"wing":"w","room":"r","content":"c"}"#;
        let req: AddDrawerRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.wing, "w");
        assert!(req.source_file.is_none());
        assert!(req.added_by.is_none());
    }

    #[test]
    fn check_duplicate_request_sparse_json_deserialises() {
        let raw = r#"{"content":"some text"}"#;
        let req: CheckDuplicateRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.content, "some text");
        assert!(req.threshold.is_none());
    }

    #[test]
    fn list_drawers_query_sparse_json_deserialises() {
        let raw = r#"{}"#;
        let q: ListDrawersQuery = serde_json::from_str(raw).unwrap();
        assert!(q.wing.is_none());
        assert!(q.cursor.is_none());
    }

    #[test]
    fn kg_requests_round_trip() {
        let add = KgAddFactRequest {
            subject: "Alice".to_owned(),
            predicate: "works_on".to_owned(),
            object: "MemPalace".to_owned(),
            valid_from: Some("2026-01-01".to_owned()),
        };
        let json = serde_json::to_string(&add).unwrap();
        let decoded: KgAddFactRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(add, decoded);

        let inv = KgInvalidateRequest {
            subject: "Alice".to_owned(),
            predicate: "works_on".to_owned(),
            object: "MemPalace".to_owned(),
            ended: None,
        };
        let json = serde_json::to_string(&inv).unwrap();
        let decoded: KgInvalidateRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(inv, decoded);
    }

    #[test]
    fn changes_response_round_trips() {
        let original = ChangesResponse {
            events: vec![ChangeEventDto {
                event_type: "drawer_added".to_owned(),
                occurred_at: "2026-01-01T00:00:00Z".to_owned(),
                entity_id: "drw_abc".to_owned(),
                actor: Some("claude".to_owned()),
                details: Some(json!({"wing":"wing_code","room":"backend"})),
            }],
            next_cursor: Some("cursor_opaque".to_owned()),
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: ChangesResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn error_body_round_trips() {
        let err =
            ErrorBody { code: "not_found".to_owned(), message: "Drawer not found".to_owned() };
        let json = serde_json::to_string(&err).unwrap();
        let decoded: ErrorBody = serde_json::from_str(&json).unwrap();
        assert_eq!(err, decoded);
    }

    #[test]
    fn ingest_batch_request_round_trips() {
        let original = IngestBatchRequest {
            wing: "wing_myproject".to_owned(),
            repo_id: "github.com/acme/myrepo".to_owned(),
            agent: Some("claude".to_owned()),
            commit_hash: Some("abc123def456".to_owned()),
            files: vec![IngestFileDto {
                relative_path: "src/main.rs".to_owned(),
                content_hash: "contenthash1".to_owned(),
                file_hash: Some("filehash1".to_owned()),
                chunks: vec![IngestChunkDto {
                    chunk_index: 0,
                    room: "backend".to_owned(),
                    text: "fn main() {}".to_owned(),
                    byte_start: Some(0),
                    byte_end: Some(12),
                    line_start: Some(1),
                    line_end: Some(1),
                }],
            }],
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: IngestBatchRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn ingest_batch_response_round_trips() {
        let original = IngestBatchResponse {
            files: vec![
                IngestFileResult {
                    relative_path: "src/main.rs".to_owned(),
                    status: "ingested".to_owned(),
                    drawers_written: 3,
                    error: None,
                },
                IngestFileResult {
                    relative_path: "src/lib.rs".to_owned(),
                    status: "skipped_unchanged".to_owned(),
                    drawers_written: 0,
                    error: None,
                },
                IngestFileResult {
                    relative_path: "src/broken.rs".to_owned(),
                    status: "failed".to_owned(),
                    drawers_written: 0,
                    error: Some("embedding failed".to_owned()),
                },
            ],
            warnings: vec!["no checkout configured for wing 'wing_x'".to_owned()],
        };
        let json = serde_json::to_string(&original).unwrap();
        let decoded: IngestBatchResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn ingest_batch_request_sparse_json_deserialises() {
        // Only required fields present; optional fields default to None / empty.
        let raw = r#"{
            "wing": "wing_proj",
            "repo_id": "github.com/org/repo",
            "files": [
                {
                    "relative_path": "README.md",
                    "content_hash": "ch1",
                    "chunks": [
                        {"chunk_index": 0, "room": "docs", "text": "hello"}
                    ]
                }
            ]
        }"#;
        let req: IngestBatchRequest = serde_json::from_str(raw).unwrap();
        assert_eq!(req.wing, "wing_proj");
        assert_eq!(req.repo_id, "github.com/org/repo");
        assert!(req.agent.is_none());
        assert!(req.commit_hash.is_none());
        assert_eq!(req.files.len(), 1);
        assert!(req.files[0].file_hash.is_none());
        let chunk = &req.files[0].chunks[0];
        assert_eq!(chunk.chunk_index, 0);
        assert!(chunk.byte_start.is_none());
        assert!(chunk.byte_end.is_none());
        assert!(chunk.line_start.is_none());
        assert!(chunk.line_end.is_none());
    }

    #[test]
    fn ingest_batch_response_sparse_json_deserialises() {
        // Only required fields; drawers_written defaults to 0, warnings to empty vec.
        let raw = r#"{
            "files": [
                {"relative_path": "src/main.rs", "status": "ingested"}
            ]
        }"#;
        let resp: IngestBatchResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.files.len(), 1);
        assert_eq!(resp.files[0].drawers_written, 0);
        assert!(resp.files[0].error.is_none());
        assert!(resp.warnings.is_empty());
    }
}
