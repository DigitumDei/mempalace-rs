# Rust v1 Release Scope

This document defines the first Rust release surface for `mempalace-rs`.

## In Scope

### CLI binaries

- `mempalace-cli`
- `mempalace-mcp`

### CLI commands frozen for v1

- `init`
- `mine`
- `search`
- `status`
- `wake-up`

### Storage shape frozen for v1

- Palace root contains `storage.sqlite3` for operational state.
- Palace root contains `lancedb/` for drawer vectors and retrieval data.

### Runtime profiles frozen for v1

- `balanced`
- `low_cpu`

### MCP tool surface frozen for v1

- `mempalace_status`
- `mempalace_list_wings`
- `mempalace_list_rooms`
- `mempalace_get_taxonomy`
- `mempalace_get_aaak_spec`
- `mempalace_kg_query`
- `mempalace_kg_add`
- `mempalace_kg_invalidate`
- `mempalace_kg_timeline`
- `mempalace_kg_stats`
- `mempalace_traverse`
- `mempalace_find_tunnels`
- `mempalace_graph_stats`
- `mempalace_search`
- `mempalace_check_duplicate`
- `mempalace_add_drawer`
- `mempalace_delete_drawer`
- `mempalace_diary_write`
- `mempalace_diary_read`

## Explicitly Deferred Or Out Of Scope

- CLI `split` is deferred. It remains visible in help and fails with an explicit deferral message.
- CLI `compress` is deferred. It remains visible in help and fails with an explicit deferral message.
- AAAK reverse parsing is deferred for Rust v1.
- Automatic Wikipedia or other networked entity enrichment is out of scope.
- Python-era state inspection and import are not part of the default Rust release scope unless Phase 10 is explicitly reopened.
- OS-native installers or package-manager distributions are not defined here; the current release artifact is the Cargo-built binary set.

## Known Limitations

- Final benchmark and low-CPU signoff must be performed on the reference environment, not assumed from a generic VM.
- `init` performs embedding startup validation and reports the resulting status, but model acquisition is still an operator-managed step.
- Low-CPU mode clamps ingest, search, and wake-up limits; it is a product mode, not a claim that every host will meet target budgets automatically.

## Release Rule

If a behavior is not documented in this directory and is not covered by the frozen command or tool surface above, it should not be treated as a Rust v1 release promise.
