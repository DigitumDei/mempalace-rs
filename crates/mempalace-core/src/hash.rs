use crate::ids::{DrawerId, IdError, RoomId, WingId};

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

/// Derive the canonical drawer id for a mined chunk.
///
/// Formula: `{wing}/{room}/{first-12-of-hash_text(source_key)}-{chunk_index:04}`
///
/// This is the single source of truth for mined drawer id derivation.  Both the
/// local ingest path and the server-side batch-ingest route must call this
/// function so that drawer ids are byte-identical across clients and servers
/// that push the same repository.
pub fn mined_drawer_id(
    wing: &WingId,
    room: &RoomId,
    source_key: &str,
    chunk_index: u32,
) -> Result<DrawerId, IdError> {
    let source_hash = &hash_text(source_key)[..12];
    DrawerId::new(format!(
        "{}/{}/{}-{:04}",
        wing.as_str(),
        room.as_str(),
        source_hash,
        chunk_index
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{mined_drawer_id, hash_text};
    use crate::ids::{RoomId, WingId};

    #[test]
    fn mined_drawer_id_format_is_stable() {
        let wing = WingId::new("wing_myproject").unwrap();
        let room = RoomId::new("backend.auth").unwrap();
        // Pin the exact id for a known input so that any accidental formula
        // change is caught at test time.
        let source_key = "projects:wing_myproject:abc123def456:src/auth.rs";
        let expected_hash = &hash_text(source_key)[..12];
        let expected = format!("wing_myproject/backend.auth/{expected_hash}-0002");

        let id = mined_drawer_id(&wing, &room, source_key, 2).unwrap();
        assert_eq!(id.as_str(), expected);
    }

    #[test]
    fn mined_drawer_id_chunk_index_zero_pads_to_four_digits() {
        let wing = WingId::new("wing_x").unwrap();
        let room = RoomId::new("room_y").unwrap();
        let id = mined_drawer_id(&wing, &room, "some:key", 0).unwrap();
        // Last segment must end in "-0000"
        assert!(id.as_str().ends_with("-0000"), "got: {}", id.as_str());
    }
}
