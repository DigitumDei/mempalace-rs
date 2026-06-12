//! Source-locator types and resolver for locator-backed [`DrawerRecord`]s.
//!
//! A [`SourceLocator`] records byte and line ranges into the original file so
//! that the stored chunk text can be re-derived lazily from the checkout on the
//! owning palace host instead of persisting the full text verbatim.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::hash::hash_bytes;
use crate::DrawerRecord;

/// Pinpoints a chunk within a source file on the owning palace host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceLocator {
    /// Byte offset of the first byte of the chunk in the original file (inclusive).
    pub byte_start: u64,
    /// Byte offset one past the last byte of the chunk (exclusive).
    pub byte_end: u64,
    /// 1-based line number of the first line of the chunk.
    pub line_start: u32,
    /// 1-based line number of the last line of the chunk.
    pub line_end: u32,
    /// BLAKE3 hex digest of the full original file bytes at mine time.
    pub file_hash: String,
    /// Absolute path of the checkout root on the palace host that owns this row.
    pub resolve_root: String,
    /// Git commit SHA at mine time, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_hash: Option<String>,
}

/// Result of resolving a [`SourceLocator`] against the local file system.
pub struct ResolvedSnippet {
    /// The resolved (or best-effort / placeholder) text.
    pub text: String,
    /// `true` when the file has changed since mining or is missing.
    pub stale: bool,
}

/// Resolve a single locator for `source_file` (relative to `locator.resolve_root`).
///
/// Resolution is always palace-side (std::fs only, no async).
///
/// * If the file is readable and its hash matches `locator.file_hash`, the byte
///   slice `byte_start..byte_end` is returned as-is (`stale: false`).
/// * If the hash mismatches: `stale: true`; if the range is still in-bounds and
///   valid UTF-8 the current bytes are returned, otherwise a placeholder.
/// * If the file is missing or unreadable: `stale: true` + placeholder.
pub fn resolve_locator(locator: &SourceLocator, source_file: &str) -> ResolvedSnippet {
    let path = Path::new(&locator.resolve_root).join(source_file);
    let bytes = std::fs::read(&path).ok();
    let current_hash = bytes.as_deref().map(hash_bytes);
    resolve_from_bytes(locator, source_file, bytes.as_deref(), current_hash.as_deref())
}

/// Shared resolution core for [`resolve_locator`] and [`resolve_records`].
///
/// `bytes`/`current_hash` are `None` when the file is missing or unreadable.
fn resolve_from_bytes(
    locator: &SourceLocator,
    source_file: &str,
    bytes: Option<&[u8]>,
    current_hash: Option<&str>,
) -> ResolvedSnippet {
    let Some(bytes) = bytes else {
        return ResolvedSnippet {
            text: format!("[stale] {source_file} missing; re-run mine"),
            stale: true,
        };
    };

    let start = locator.byte_start as usize;
    let end = locator.byte_end as usize;
    let slice_text =
        bytes.get(start..end).and_then(|slice| std::str::from_utf8(slice).ok()).map(str::to_owned);

    // Hash match — slice must be valid UTF-8 by construction; if bounds or UTF-8
    // fail anyway (defensive, never panic), fall through to the stale path.
    if current_hash == Some(locator.file_hash.as_str()) {
        if let Some(text) = slice_text {
            return ResolvedSnippet { text, stale: false };
        }
    }

    // Hash mismatch or defensive failure: best-effort current text, else placeholder.
    let text = slice_text
        .unwrap_or_else(|| format!("[stale] {source_file} changed since mining; re-run mine"));
    ResolvedSnippet { text, stale: true }
}

/// Resolve locators for every record in `records` that has `locator: Some(_)`,
/// rewriting `record.content` with the resolved text in place.
///
/// Returns a `Vec<bool>` parallel to `records` where `true` means the resolved
/// snippet was stale (or there was no locator — always `false` for those).
///
/// File reads and hashes are memoised per `(resolve_root, source_file)` pair
/// within a single call so N chunks from the same file incur only one read.
pub fn resolve_records(records: &mut [DrawerRecord]) -> Vec<bool> {
    // Memo: (resolve_root, source_file) -> bytes + hash (None when unreadable),
    // so N chunks from the same file incur one read and one digest.
    let mut file_cache: HashMap<(String, String), Option<(Vec<u8>, String)>> = HashMap::new();

    let mut stale_flags = Vec::with_capacity(records.len());

    for record in records.iter_mut() {
        let Some(locator) = record.locator.as_ref() else {
            stale_flags.push(false);
            continue;
        };

        let cache_key = (locator.resolve_root.clone(), record.source_file.clone());
        let cached = file_cache.entry(cache_key).or_insert_with(|| {
            let path = Path::new(&locator.resolve_root).join(&record.source_file);
            std::fs::read(&path).ok().map(|bytes| {
                let digest = hash_bytes(&bytes);
                (bytes, digest)
            })
        });

        let snippet = resolve_from_bytes(
            locator,
            &record.source_file,
            cached.as_ref().map(|(bytes, _)| bytes.as_slice()),
            cached.as_ref().map(|(_, digest)| digest.as_str()),
        );

        stale_flags.push(snippet.stale);
        record.content = snippet.text;
    }

    stale_flags
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::io::Write as _;

    use tempfile::NamedTempFile;
    use time::macros::datetime;

    use super::*;
    use crate::{DrawerId, RoomId, WingId};

    fn make_locator(
        root: &str,
        file_hash: &str,
        byte_start: u64,
        byte_end: u64,
    ) -> SourceLocator {
        SourceLocator {
            byte_start,
            byte_end,
            line_start: 1,
            line_end: 1,
            file_hash: file_hash.to_owned(),
            resolve_root: root.to_owned(),
            commit_hash: None,
        }
    }

    fn make_record(
        id: &str,
        source_file: &str,
        content: &str,
        locator: Option<SourceLocator>,
    ) -> DrawerRecord {
        DrawerRecord {
            id: DrawerId::new(id).unwrap(),
            wing: WingId::new("wing").unwrap(),
            room: RoomId::new("room").unwrap(),
            hall: None,
            date: None,
            source_file: source_file.to_owned(),
            chunk_index: 0,
            ingest_mode: "test".to_owned(),
            extract_mode: None,
            added_by: "tester".to_owned(),
            filed_at: datetime!(2026-01-01 00:00:00 UTC),
            importance: None,
            emotional_weight: None,
            weight: None,
            content: content.to_owned(),
            content_hash: "hash".to_owned(),
            embedding: vec![],
            locator,
        }
    }

    #[test]
    fn resolve_locator_exact_match() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let path = file.path();
        let hash = crate::hash::hash_bytes(b"hello world");
        let root = path.parent().unwrap().to_str().unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();
        let locator = make_locator(root, &hash, 0, 5);
        let snippet = resolve_locator(&locator, name);
        assert!(!snippet.stale);
        assert_eq!(snippet.text, "hello");
    }

    #[test]
    fn resolve_locator_stale_on_hash_mismatch() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"hello world").unwrap();
        let path = file.path();
        let root = path.parent().unwrap().to_str().unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();
        let locator = make_locator(root, "deadbeef", 0, 5);
        let snippet = resolve_locator(&locator, name);
        assert!(snippet.stale);
        // Best-effort: range is in-bounds and valid UTF-8
        assert_eq!(snippet.text, "hello");
    }

    #[test]
    fn resolve_locator_missing_file() {
        let locator = make_locator("/nonexistent/root", "deadbeef", 0, 5);
        let snippet = resolve_locator(&locator, "ghost.txt");
        assert!(snippet.stale);
        assert!(snippet.text.contains("missing"));
    }

    #[test]
    fn resolve_records_memoizes_file_read() {
        // Write a single file, create two records pointing to it.
        let mut file = NamedTempFile::new().unwrap();
        let content = b"abcdefghij";
        file.write_all(content).unwrap();
        let path = file.path();
        let hash = crate::hash::hash_bytes(content);
        let root = path.parent().unwrap().to_str().unwrap();
        let name = path.file_name().unwrap().to_str().unwrap();

        let mut records = vec![
            make_record(
                "wing/room/0001",
                name,
                "",
                Some(make_locator(root, &hash, 0, 3)),
            ),
            make_record(
                "wing/room/0002",
                name,
                "",
                Some(make_locator(root, &hash, 3, 6)),
            ),
            make_record("wing/room/0003", name, "no locator", None),
        ];

        let stale = resolve_records(&mut records);
        assert_eq!(stale, vec![false, false, false]);
        assert_eq!(records[0].content, "abc");
        assert_eq!(records[1].content, "def");
        assert_eq!(records[2].content, "no locator"); // untouched
    }

    #[test]
    fn resolve_records_stale_missing_file() {
        let mut records = vec![make_record(
            "wing/room/0001",
            "ghost.txt",
            "",
            Some(make_locator("/nonexistent", "deadbeef", 0, 5)),
        )];
        let stale = resolve_records(&mut records);
        assert_eq!(stale, vec![true]);
        assert!(records[0].content.contains("missing"));
    }
}
