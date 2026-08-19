# Delegation-loop telemetry

MemPalace records what a delegated agent run declared it was allowed to spend, what it actually
did, and why it stopped — durably, in the palace's local `storage.sqlite3`, and without storing
transcripts. The host agent runtime still spawns workers, schedules models, executes tools, and
**enforces budgets during execution**. MemPalace stores the declarations, checkpoints, and
outcomes so a run stays reconstructable after a restart. Telemetry is local-only and is not
federated.

This is the second half of the Phase 2 work described in
[Coordination Phase 2 Design](Coordination-Phase-2-Design.md); the first half is the
[Skill Registry](Skill-Registry.md).

## Data and guarantees

- A **span** is one delegated run. It always references a coordination task — the task tree in
  [Native Coordination](Coordination.md) stays the single source of truth for what work exists,
  so there is no second, competing record of the work itself.
- Spans nest through `parent_span_id`, forming the delegation tree.
- **`depth` and `fan_out_index` are derived, not caller-supplied.** `depth` is the parent's depth
  plus one; `fan_out_index` is the number of siblings that already existed. A host comparing
  recorded depth against its declared `max_depth` is comparing against something the caller could
  not have misreported.
- A **checkpoint** is an append-only note about what happened within a span, ordered by a local
  sequence.
- Span starts and checkpoint appends deduplicate only on `(actor, idempotency_key)`. Replaying a
  key returns the committed record.
- Closing a span takes an expected revision. A stale revision returns an explicit conflict; a
  span that is already terminal cannot be closed again.
- Exact retrieval of a span or checkpoint by ID returns `found: false` on a miss and never falls
  back to semantic search.

Span statuses are `running`, `completed`, `failed`, `cancelled`, and `expired`. Only `running`
is non-terminal, and it is not a valid closing status.

## Budgets are stored, never enforced

`budgets` is opaque JSON — declared maxima for depth, fan-out, turns, tool calls, tokens, wall
time, retries, and concurrency. MemPalace persists it and never acts on it. Nothing in this
module refuses work for exceeding a budget; that decision belongs to the host runtime, per the
product boundary in [issue #98](https://github.com/DigitumDei/mempalace-rs/issues/98).

What MemPalace *does* guarantee is that curtailment stays visible. Every close records an
explicit `stop_reason`:

`completed`, `budget_exhausted`, `max_depth_reached`, `max_fan_out_reached`, `cancelled`,
`error`, `human_stop`

A run that ran out of budget is therefore distinguishable from one that merely never finished
being written down.

**Repeated delegation** is visible the same way: `mempalace_delegation_spans_for_task` returns
every span for a task, so a second root span against already-delegated work shows up as data
rather than being silently absorbed.

## Checkpoints are bounded on purpose

`summary` is capped at **8 KiB** — far below the 1 MiB coordination payload cap. A checkpoint is
a note about what happened, not the thing that happened. Anything larger belongs in an immutable
coordination artifact, referenced by `artifact_ref`; artifacts are already content-hashed and
exact-ID retrievable.

This cap is the enforceable half of "sensitive complete traces and secrets are not persisted by
default": it makes storing a transcript structurally impractical. Not storing *secrets* remains
a caller responsibility, exactly as actor authentication does — MemPalace cannot detect them.

## Reconstructing a run

`mempalace_delegation_trace` returns the root span, every descendant span, and each of their
checkpoints in sequence order. The node list is **flat**, with each node carrying
`parent_span_id`, so a consumer rebuilds the tree itself and the response imposes no recursion
depth. This is the export path for trace visualization; MemPalace does not ship a viewer.

Because spans, checkpoints, and artifacts are all durable and exact-ID retrievable, a run
reconstructs after a process restart from stored references alone — no prior conversation
context is required.

## MCP tools

- `mempalace_delegation_span_start`, `mempalace_delegation_span_get`,
  `mempalace_delegation_span_close`, `mempalace_delegation_spans_for_task`
- `mempalace_delegation_checkpoint_append`, `mempalace_delegation_checkpoint_get`
- `mempalace_delegation_trace`

`mempalace_delegation_span_close` returns `{"success": false, "conflict": {...}}` on a revision
mismatch, matching the skill-registry, lineage, and self-observation tools.

## Recovery and maintenance

Delegation tables participate in the same SQLite WAL and backup procedures as other operational
state. Back up `storage.sqlite3` consistently with the palace. No semantic index rebuild is
needed for telemetry records.
