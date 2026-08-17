# Coordination Phase 0 experiment

This document records the local-first experiment from issue #99. It evaluates whether the existing drawer, search, and change-cursor tools can support durable agent coordination without new storage, APIs, or an agent runtime.

## Reproduce

Install the repository skill by copying `skills/coordinate-with-mempalace` into a supported client's skill directory, or invoke it directly from the checkout. Validate the package and its envelopes:

```bash
python3 skills/coordinate-with-mempalace/scripts/validate_envelope.py \
  skills/coordinate-with-mempalace/assets/*.json
```

The package also passes the standard Codex skill validator. It contains runnable, tool-level procedures for manager-as-tools and explicit-handoff workflows and uses only `mempalace_add_drawer`, `mempalace_get_changes_since`, and `mempalace_search`.

## Recorded runs — 2026-08-17

Both runs targeted `wing_mempalace_rs/workflows`. Routing placed the drawers on configured origin `actuarius`.

| Measure | Manager as tools | Explicit handoff |
|---|---:|---:|
| Tasks attempted/completed | 1 / 1 | 1 / 1 |
| Completion rate | 100% | 100% |
| Duplicate execution | 0 | 0 |
| Expected change events observed | 3 / 3 | 5 / 5 |
| Lost updates observed | 0 | 0 |
| Semantic false-duplicate write rejections | 0 | 1 |
| Approximate tool latency | 17.4 s | 28.9 s including retry |
| Tokens | not available from the tool harness | not available from the tool harness |
| Restart/content recovery without transcript | not exercised | content found by unique search |
| Authoritative recovery by reference | no | no |

Manager-as-tools references:

- task: `drawer_wing_mempalace_rs_workflows_2e9c0b3e78c3d2ea`
- artifact: `drawer_wing_mempalace_rs_workflows_cdc1fa754720c364`
- result: `drawer_wing_mempalace_rs_workflows_ce55519c7b0d8352`

Explicit-handoff references:

- task: `drawer_wing_mempalace_rs_workflows_ef5611209f835e5c`
- handoff: `drawer_wing_mempalace_rs_workflows_cf7fe18553b40196`
- acknowledgement: `drawer_wing_mempalace_rs_workflows_838dbf13c0c95e98`
- artifact: `drawer_wing_mempalace_rs_workflows_02ba6c6cb212b709`
- result: `drawer_wing_mempalace_rs_workflows_278f40252031eaba`

Cursor pagination was exercised twice. The manager run used limit 2 and origin `actuarius` returned opaque cursor `2026-08-17T10:31:18.309442004Z|224`; supplying it returned the remaining result event. The handoff run used limit 3 and returned `2026-08-17T10:32:22.108719845Z|228`; supplying it returned the artifact and result. Cursors are recorded as opaque values, not parsed or synthesized by the workflow.

The manager artifact contains 238 bytes and was replaced in downstream communication by a roughly 160-byte structured reference. This small sample does not save bytes, but reference size stays bounded as artifacts grow. The handoff artifact contains 259 bytes. Full transcript size was not exposed by the harness, so no token-saving claim is made.

## Failure and limitation report

The experiment is race-prone and is not a reliable queue:

- No atomic claim, compare-and-set, lease, acknowledgement state, or ordered mailbox prevents duplicate work or lost ownership updates.
- `mempalace_get_changes_since` provides useful discovery and opaque per-origin continuation, but observing an event does not acknowledge delivery.
- The MCP surface has no get-drawer-by-ID operation. `mempalace_search` recovered the restart artifact content, but its result omitted the drawer ID. A consumer therefore cannot prove that content came from the stable reference it received.
- Semantic search ranking is not authoritative delivery. A relevant envelope can fall outside the result limit or be displaced by unrelated content.
- Duplicate detection is semantic and rejected the first explicit-handoff result as a duplicate of its acknowledgement at similarity 0.9117. Rewording the result allowed the write, demonstrating content-dependent delivery failure.
- Cross-origin timestamps are not an ordering mechanism. Clients must preserve each origin's opaque cursor.
- Payload hashes detect changed artifact content but do not authenticate producer identity.
- Tokens were unavailable, and the two-run sample is too small for performance or reliability claims.

## Design review and decision

Decision: **GO for Phase 1 native local coordination**, while keeping Phase 0 explicitly experimental. Existing primitives demonstrate useful durability and cursor-based discovery, but they cannot meet authoritative reference recovery or race-safe delivery requirements.

Phase 1 should add local, transactional coordination storage and purpose-built MCP tools:

1. `coordination_tasks`: immutable task identity plus revision, status, owner, lease expiry, idempotency key, and created/updated sequence.
2. `coordination_messages`: append-only per-task mailbox with monotonic local sequence, sender, recipient, kind, envelope version, acknowledgement state, and deduplication key.
3. `coordination_artifacts`: immutable artifact metadata and content/hash, retrievable by exact ID.
4. Atomic operations for create, claim/renew/release, transition with expected revision, append message, acknowledge, put/get artifact, and list changes after an opaque cursor.
5. Transactional outbox/change-log rows committed with every mutation so polling cannot miss a committed state change.
6. Local authorization boundaries, bounded payloads, expiry/cleanup policy, and audit fields. Federation and agent execution remain out of scope.

Required semantics are at-least-once discovery with idempotent writes, exact ID retrieval, optimistic concurrency, explicit acknowledgements, and deterministic local ordering. Exactly-once execution is not promised.

## Exit assessment

- Versioned task, handoff, result, and artifact envelopes: met.
- Both reference workflows use existing tools and reach a persisted result: met, with one false-duplicate retry.
- Content survives a simulated fresh-context lookup without transcript copying: met experimentally.
- Results passed by stable reference: met at the producer/transport boundary.
- Stable references can be authoritatively dereferenced: not met; this is the primary Phase 1 requirement.
- Measurements and known limitations: met.
- Go/no-go design review: met; decision is GO.
