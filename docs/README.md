# Documentation

This directory documents the Rust release surface that exists in `mempalace-rs/` today.

### Getting started

- [Quickstart](Quickstart.md) — install, initialize, mine, search, connect an MCP client

### Reference

- [Release Scope](Release-Scope.md) — what is in and out of the shipped surface
- [Coordination Phase 0](Coordination-Phase-0.md) — experimental coordination skill, measurements, limitations, and Phase 1 decision
- [Native Coordination](Coordination.md) — transactional tasks, messages, results, artifacts, leases, cursors, and recovery
- [Skill Registry](Skill-Registry.md) — versioned, governed reusable procedures with scope-gated promotion and outcome history
- [Delegation Telemetry](Delegation-Telemetry.md) — delegated-run spans, derived depth/fan-out, bounded checkpoints, stop reasons, trace export
- [External Agent Task Handoffs](Agent-Task-Handoffs.md) — launching OpenCode from durable tasks, supervised review, restart recovery, and observed rough edges
- [Coordination Phase 2 Design](Coordination-Phase-2-Design.md) — the design proposal behind the skill registry and delegation telemetry
- [Coordination Phase 3 Design](Coordination-Phase-3-Design.md) — the design proposal for opt-in federated coordination, scoped tokens, and the A2A and MCP Tasks adapters
- [CLI Surface](CLI-Surface.md) — every `mempalace-cli` command and flag
- [Config Schema](Config-Schema.md) — `config.json`, `projects.json`, `mempalace.yaml`, env overrides
- [Mined Storage](Mined-Storage.md) — locator model, repository views, stale semantics, discovery rules
- [Self-Continuity Across Models](Self-Continuity.md) — lineages, reviewed self-observations,
  identity packets, and model/harness migrations
- [Federation](Federation.md) — server, client routing, federated and branch-aware mining

### Operations

- [Standard Operator Guide](Operator-Standard.md) — deployment, maintenance, recovery, troubleshooting
- [Low-CPU Operator Guide](Operator-Low-CPU.md) — constrained environments
- [Cloud Environment](Cloud-Environment.md) — building and testing in a cloud sandbox or CI runner
- [Release Operations](Release-Operations.md) — signed candidate and stable release runbook
- [Packaging And Validation](Packaging-And-Validation.md) — release artifacts and gate rows

### Historical

- [Validation Evidence](Validation-Evidence.md) — a dated record of one validation pass, kept as evidence rather than as current reference

These documents are derived from the current implementation rather than from a migration wish
list. Anything not documented here should be treated as out of scope unless the docs are
updated explicitly — and per [CLAUDE.md](../CLAUDE.md), a behaviour change and its
documentation update belong in the same change.
