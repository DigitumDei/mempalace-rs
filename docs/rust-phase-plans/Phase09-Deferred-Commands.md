# Phase 9 — deferred CLI commands

The Rust v1 CLI keeps `split` and `compress` visible in command help so existing
automation receives an explicit response rather than an unknown-command error.
They are not implemented commands in the shipped release surface.

## `split`

The Python-era `split` workflow divided accumulated conversation material into
session-level records. The Rust ingestion and locator model do not yet define a
compatible split policy, particularly for preserving source identity,
repository views, and durable project keys. The command therefore returns a
non-zero result without changing the palace.

## `compress`

The Python-era `compress` workflow rewrote stored material into a more compact
representation. Rust currently treats authored drawers, mined locators, and
operational records as separate storage contracts; no safe general-purpose
compression rewrite has been approved for the v1 surface. The command returns
a non-zero result without changing the palace.

## Reopening the decision

Implementing either command requires a new design decision covering source-key
identity, stale-row behavior, recovery, dry-run guarantees, and compatibility
with repository views and authored memory. Until that decision is made, use
the supported ingestion, search, maintenance, and scoped `prune` commands.

See [Release Scope](../Release-Scope.md#explicitly-deferred-or-out-of-scope) for
the public v1 boundary and [CLI Surface](../CLI-Surface.md#deferred-commands)
for the command behavior.
