/// Blake3 hex hashing utilities shared across MemPalace crates.
///
/// These are the canonical implementations. Duplicate private helpers in
/// `mempalace-ingest`, `mempalace-mcp`, and `mempalace-server` delegate here.
pub fn hash_bytes(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

pub fn hash_text(text: &str) -> String {
    hash_bytes(text.as_bytes())
}
