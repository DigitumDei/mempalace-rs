# Rust migration notes

Phase 0 locks the Python reference behavior before any Rust rewrite starts.

We decided to harvest fixtures first because search formatting, wake-up text,
and MCP payload shapes are part of the product surface. The migration roadmap
should preserve CLI parity, MCP tool names, and graph field names even if the
storage backend changes.

The project plan prefers LanceDB for drawers and SQLite for operational state.
