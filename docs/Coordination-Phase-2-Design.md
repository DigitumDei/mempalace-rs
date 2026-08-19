# Coordination Phase 2 — design proposal

**Status: proposal, not implemented.** This document proposes a storage and MCP surface
design for [issue #101](https://github.com/DigitumDei/mempalace-rs/issues/101), the way
[Coordination-Phase-0.md](Coordination-Phase-0.md) proposed the Phase 1 design that shipped
in [Coordination.md](Coordination.md). Nothing described here exists in code yet; update this
doc's status line and [docs/README.md](README.md) once implementation lands, per the
documentation rule in [CLAUDE.md](../CLAUDE.md).

Phase 2 has two independent halves per issue #101: a versioned, governed **skill registry**,
and **delegation-loop telemetry**. They share no tables but do share two design precedents
already proven in this codebase, so this proposal reuses both rather than inventing new
patterns:

- The **candidate → promoted/validated → superseded/retired lifecycle with a revisioned head
  row plus an append-only review/audit table**, from `self_observations` /
  `self_observation_reviews` in [Self-Continuity.md](Self-Continuity.md)
  (`crates/mempalace-storage/src/sqlite.rs`).
- The **immutable head row with optimistic-concurrency revision plus an append-only event log
  written in the same transaction as every mutation**, from `coordination_tasks` /
  `coordination_events` in [Coordination.md](Coordination.md)
  (`crates/mempalace-storage/src/coordination.rs`).

Both halves stay local-only, consistent with issue #98's phased federation boundary — no
change to `mempalace-federation` or routing is proposed here.

## Skill registry

### Data model

```
skills (head, revisioned)
  skill_id            TEXT   -- stable name-derived identity, e.g. "coordinate-with-mempalace"
  version              INTEGER  -- monotonic per skill_id, starting at 1
  scope                TEXT   -- 'agent' | 'project' | 'organization'
  status               TEXT   -- 'candidate' | 'validated' | 'promoted' | 'superseded' | 'retired'
  applicability         TEXT   -- trigger/applicability description
  instructions_ref      TEXT   -- resource reference (drawer ID, file path, or inline text)
  required_capabilities JSON
  required_tools        JSON
  required_permissions  JSON
  author                TEXT   -- actor asserted by host runtime
  provenance            JSON   -- source session/drawer references
  confidence             REAL
  supersedes_skill_id    TEXT, supersedes_version INTEGER
  revision               INTEGER
  created_at, updated_at TEXT

skill_reviews (append-only)
  review_id, skill_id, version, from_status, to_status, reviewer, reason, occurred_at

skill_outcomes (append-only)
  outcome_id, skill_id, version, task_id NULL, result  -- 'success' | 'failure' | 'partial'
  evaluator, notes, recorded_by, recorded_at
```

`(skill_id, version)` is the stable, exact-ID-retrievable reference — mirrors the Phase 1
requirement that exact-ID retrieval, not semantic search, is authoritative. `skill_id` alone
resolves to the current promoted (or latest) version for discovery.

### Lifecycle rules

1. `skill_propose` creates a new `candidate` version. A first proposal for a new `skill_id`
   starts at version 1; a revision to an existing skill proposes `version = max(version) + 1`
   with `supersedes_skill_id`/`supersedes_version` set — this mirrors how
   `review_self_observation` requires the superseded row to currently be `promoted` before it
   can transition, enforced with the same CAS-on-revision pattern.
2. `skill_attach_validation` records test or evaluator results against a candidate. This is
   additive (append to `skill_outcomes` with no `task_id`), not a status change.
3. `skill_promote` transitions `candidate` (or `validated`, if the validation step is modeled
   as its own status) to `promoted`. Enforce server-side, not just by convention:
   - `scope = agent`: the author may self-promote (mirrors that individual lineages already
     self-govern their own self-observations).
   - `scope = project` or `organization`: promotion requires at least one recorded
     `skill_outcomes` validation row and a `reviewer` distinct from `author` — this is the
     literal acceptance criterion "candidate revisions cannot become shared authoritative
     procedures without configured validation or human approval," enforced the same way
     `review_self_observation` refuses to supersede a row that isn't currently `promoted`.
   - Promoting a new version of an already-promoted `skill_id` atomically supersedes the old
     version in the same transaction (same pattern as
     `SelfObservationStatus::Superseded` handling).
4. `skill_retire` transitions any non-terminal status to `retired` with a reason. Terminal
   states (`superseded`, `retired`) cannot transition again.
5. `skill_record_outcome` appends a `skill_outcomes` row with `task_id` set when the outcome is
   tied to a specific coordination task — this is what lets a skill's real-world success rate
   accumulate across many delegated runs.

### MCP surface (illustrative, finalize during implementation)

`mempalace_skill_propose`, `mempalace_skill_get`, `mempalace_skill_search` (filter by scope/
status, exact `(skill_id, version)` lookup only — no semantic ranking as authority, per the
same Phase 1 rule), `mempalace_skill_attach_validation`, `mempalace_skill_promote`,
`mempalace_skill_retire`, `mempalace_skill_record_outcome`.

## Delegation-loop telemetry

### Data model

```
delegation_spans (head, revisioned)
  span_id, parent_span_id NULL   -- nesting for recursive delegation
  task_id                         -- FK coordination_tasks; a span always wraps a task
  delegator, delegate             -- actors asserted by host runtime
  depth, fan_out_index             -- position in the delegation tree
  budgets   JSON  -- declared max_depth, max_fan_out, max_turns, max_tool_calls, max_tokens,
                     max_wall_time_seconds, max_retries, max_concurrency
  status                           -- 'running' | 'completed' | 'failed' | 'cancelled' | 'expired'
  stop_reason                      -- 'completed' | 'budget_exhausted' | 'max_depth_reached' |
                                       'max_fan_out_reached' | 'cancelled' | 'error' | 'human_stop'
  revision
  started_at, ended_at TEXT

delegation_checkpoints (append-only, same-transaction-as-mutation like coordination_events)
  checkpoint_id, span_id, sequence
  checkpoint_type   -- 'turn' | 'tool_call' | 'token_usage' | 'retry' | 'human_approval' | 'claim' | 'handoff'
  summary            -- bounded text (reuse Phase 1's payload size cap), never a full transcript
  artifact_ref NULL  -- FK coordination_artifacts, for anything too large for `summary`
  actor, occurred_at
```

This is deliberately the same shape as `coordination_tasks` + `coordination_events`: a
revisioned mutable head for current state, an append-only log for history, budgets and
outcomes as data MemPalace stores rather than enforces. Per issue #98's product boundary and
issue #101's constraints, **MemPalace persists declared budgets and observed checkpoints; the
host runtime enforces live budgets during execution.** No live-enforcement code is proposed
here.

A span reconstructs a delegated run without transcripts: replay `delegation_checkpoints` in
sequence order, following `artifact_ref` for any checkpoint that needs full content, exactly
as Phase 1's restart recovery replays `coordination_events` and dereferences artifacts by
exact ID.

### Trace export

`mempalace_delegation_trace_get(span_id)` returns the span, its full checkpoint sequence, and
child span IDs as one structured JSON document — this satisfies "trace data can be exported or
visualized" without MemPalace owning a visualization UI; host tooling or an external viewer
consumes the export. `mempalace_delegation_trace_export` for a whole task (root span + all
descendants) is a thin wrapper over the same query, useful for post-hoc analysis after a task
completes.

### MCP surface (illustrative)

`mempalace_delegation_span_start`, `mempalace_delegation_checkpoint_append`,
`mempalace_delegation_span_get`, `mempalace_delegation_span_transition` (CAS on revision, same
pattern as `mempalace_task_transition`), `mempalace_delegation_trace_get`.

## Explicit non-goals (carried from issue #98/#101)

- No live budget enforcement — that stays in the host runtime.
- No full-transcript storage — checkpoints are bounded summaries plus artifact references.
- No automatic skill promotion — shared-scope promotion always requires recorded validation or
  a distinct human/reviewer approval.
- No secrets persisted by default — this is a documentation/convention constraint on callers
  (like Phase 1's actor-assertion boundary), not something MemPalace can detect and block.
- No federation change — both halves are local-only tables, matching Phase 1.

## Open questions to resolve before implementation

1. Is `validated` a distinct status between `candidate` and `promoted`, or is validation
   evidence just an unordered append to `skill_outcomes` with promotion checking for at least
   one row? The proposal above assumes the latter (fewer states, same guarantee) but should be
   confirmed against how strict the "configured validation" acceptance criterion needs to be.
2. Should `delegation_spans.task_id` be mandatory (every span wraps exactly one coordination
   task) or optional, for delegation that predates task creation? Phase 1's task model already
   has `parent_id`/`dependencies`; a mandatory FK keeps one source of truth for the delegation
   tree instead of two overlapping parent-pointer systems.
3. What is the bounded size for `delegation_checkpoints.summary`? Phase 1 caps artifact/payload
   content at 1 MiB; checkpoint summaries should likely be far smaller (a few KB) since they
   are meant to avoid transcript-scale content by construction.
4. Does skill `instructions_ref` point at a drawer, a repo file path, or support both? Phase 0's
   skill package (`skills/coordinate-with-mempalace/`) is a repo-checked-in directory, not a
   drawer — the registry needs to represent that as a first-class provenance source, not force
   everything into a drawer reference.
