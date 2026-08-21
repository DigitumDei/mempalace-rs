# Coordination Phase 3 — design proposal

**Status: proposed.** This document is the design record for
[issue #102](https://github.com/DigitumDei/mempalace-rs/issues/102), in the same role
[Coordination-Phase-2-Design.md](Coordination-Phase-2-Design.md) plays for issue #101.

**For behaviour that actually exists, read [Coordination.md](Coordination.md),
[Federation.md](Federation.md) and [Config-Schema.md](Config-Schema.md).** Where they disagree
with this document once the work has shipped, they are authoritative.

Phase 3 extends coordination across configured palaces and exposes it to external agent
protocols. Phases 1 and 2 deliberately kept every coordination, skill and delegation table
local-only. That constraint is lifted here **only** for coordination, and only when a user
configures it — the local-first invariant is preserved by making federation opt-in per wing,
exactly as drawer routing already is.

Three product decisions were taken before design (2026-08-20, with Dion):

1. **Delivery is a stack of reviewed PRs**, one per stage below, each branched off the previous.
2. **Authorization is scoped tokens across all routes**, not coordination-only. This closes
   [issue #38](https://github.com/DigitumDei/mempalace-rs/issues/38) as a consequence. Tokens
   with no `scopes` field keep unrestricted access, so existing deployments do not break.
3. **A wing lives on the task**, and messages, artifacts, results and events inherit it.

## Non-goals

- **No multi-master tasks.** A task is authoritative in exactly one palace — the one that
  created it. Coordination records are never replicated, and `write: both` is a configuration
  error on a coordination route. Claim and lease semantics require a single authority.
- **No cross-origin cancellation cascade.** Cancelling a parent emits an event the child's
  host runtime can observe; MemPalace does not reach into another palace to cancel children.
- **No live budget enforcement.** Unchanged from Phase 2 — the host runtime still owns it.
- **No streaming or push.** Polling with per-origin opaque cursors remains the baseline.
- **No adapter fields in the core schema.** Neither adapter may add a column, and neither may
  dictate an internal type.

## Stage 1 — Wing-scoped coordination (local only)

`coordination_tasks` gains a wing. Everything else derives from it.

```
coordination_tasks
  wing TEXT NOT NULL DEFAULT 'wing_unscoped'   -- new
coordination_events
  wing TEXT NOT NULL DEFAULT 'wing_unscoped'   -- new, materialised from the task at write time
```

`wing` is normalised through `WingId::normalized` (`crates/mempalace-core/src/ids.rs`) on the
way in, so `myproject` and `wing_myproject` are the same wing. This is deliberately the
opposite of the skill registry's current behaviour, where `skill_propose` stores a raw wing but
`skill_list` normalises before querying — that asymmetry is a latent bug and Stage 1 should fix
it in passing rather than copy it.

Messages, artifacts and results do **not** get a wing column; they reach it through their
mandatory `task_id`. Events do, because the event feed must be wing-filterable and
per-origin-cursored without a join, and `coordination_events.task_id` is nullable. The event's
wing is always derived from the task inside the same transaction — never supplied by a caller.

**Migration.** Coordination tables are not in the versioned `MIGRATIONS` array in
`crates/mempalace-storage/src/sqlite.rs`; they are created by `CoordinationStore::ensure_schema`
with `CREATE TABLE IF NOT EXISTS`, which runs *after* `StorageEngine::open` has applied the
versioned migrations. A versioned migration therefore cannot be used — on a fresh palace it
would run before the table exists. Instead `ensure_schema` becomes upgrade-aware: check
`PRAGMA table_info(coordination_tasks)` and, when `wing` is absent, issue

```sql
ALTER TABLE coordination_tasks ADD COLUMN wing TEXT NOT NULL DEFAULT 'wing_unscoped';
```

SQLite permits `ADD COLUMN` with `NOT NULL` when a non-null default is supplied, so no table
rebuild and no foreign-key dance is needed, and fresh and upgraded palaces end with an
identical schema. `wing_unscoped` is a reserved wing name meaning "created before wings
existed"; document it, and require a test that opens a pre-Phase-3 palace fixture and proves
existing tasks survive with that wing.

`NewTask` gains a required `wing`. This is a breaking change to `mempalace_task_create`.

**Reads become wing-filterable.** `events()` and `inbox()` gain an optional wing filter. There
is still no `list_tasks` — exact-ID and event replay remain the only discovery paths, unchanged
from Phase 1.

## Stage 2 — Scoped bearer tokens (server only)

`TokenEntry` gains an optional `scopes` array:

```json
[
  { "token": "...", "name": "alice", "enabled": true },
  { "token": "...", "name": "ci", "enabled": true,
    "scopes": [ { "wings": ["wing_myproject"], "operations": ["read", "coordination_read"] } ] }
]
```

- `scopes` absent or `null` → unrestricted. This is the grandfathering rule that keeps existing
  `server_tokens.json` files working.
- `scopes: []` → no access at all. An explicit empty list is a deliberate lockout, not a
  synonym for absent.
- `wings` accepts `"*"` for every wing. Wing entries are normalised at load with
  `WingId::normalized`, so the token file and the request agree on spelling.
- `operations` is a **closed** enum. An unrecognised operation in the token file is a load
  error, not a warning — the registry already fails closed on malformed reloads and this must
  match. The set is: `read`, `write`, `delete`, `ingest`, `coordination_read`,
  `coordination_write`, `coordination_claim`.

`coordination_claim` is separate from `coordination_write` on purpose: claiming, renewing and
transitioning a task takes a lease and can starve other workers, whereas creating a task or
filing an artifact cannot. An observer token should be able to hold `coordination_write`
without being able to seize work.

`AuthIdentity` widens from `AuthIdentity(pub String)` to carry the resolved scope set. Because
only 5 of the 17 protected handlers extract it today, authorization is enforced in a layer, not
per handler: each route declares its required operation, and the wing is resolved from the
request (path, query or body) before the handler runs. Routes whose wing is not knowable until
the body is parsed — `POST /v1/drawers`, `POST /v1/ingest/batch` — check inside the handler
instead; make that split explicit in code and cover both paths with tests.

`ServerError::Forbidden` is added, mapping to **403** with code `forbidden`. 401 keeps its
current meaning (no or invalid token); 403 means authenticated but not permitted. Do not
collapse the two.

The existing diary guard is unchanged and still applies to every token regardless of scope —
it is a content rule, not an identity rule.

## Stage 3 — Coordination over the wire (server + DTOs)

New DTOs in `mempalace-federation` mirroring the storage types, following that crate's existing
conventions: every optional field carries `#[serde(default)]`, every DTO derives
`Debug, Clone, Serialize, Deserialize, PartialEq`, and every DTO gets a round-trip test and a
sparse-JSON test.

New routes, all under the existing auth layer:

| Method + path | Operation required |
|---|---|
| `POST /v1/coordination/tasks` | `coordination_write` |
| `GET /v1/coordination/tasks/{id}` | `coordination_read` |
| `POST /v1/coordination/tasks/{id}/claim` | `coordination_claim` |
| `POST /v1/coordination/tasks/{id}/renew` | `coordination_claim` |
| `POST /v1/coordination/tasks/{id}/transition` | `coordination_claim` |
| `POST /v1/coordination/messages` | `coordination_write` |
| `GET /v1/coordination/messages/{id}` | `coordination_read` |
| `POST /v1/coordination/messages/{id}/ack` | `coordination_write` |
| `GET /v1/coordination/inbox` | `coordination_read` |
| `POST /v1/coordination/artifacts` | `coordination_write` |
| `GET /v1/coordination/artifacts/{id}` | `coordination_read` |
| `POST /v1/coordination/results` | `coordination_write` |
| `GET /v1/coordination/results/{id}` | `coordination_read` |
| `GET /v1/coordination/events` | `coordination_read` |

`"coordination"` joins the capability list in `route_info`. `FEDERATION_API_VERSION` stays at
`1` — it is a strict equality gate with no compatibility window, so bumping it would break
every existing peer in both directions. Additive change rides on `capabilities`, which is what
that list is for.

**Actor identity stops being free-form on the wire.** Today the model picks its own actor
string and storage accepts it. Over federation the authenticated token identity is
authoritative: the server derives the actor from `AuthIdentity` and applies the same prefixing
rule `route_drawers_add` already uses — a claimed name that differs from the authenticated
identity is stored as `{identity}:{claimed}`, never as the claimed name alone. A remote caller
cannot impersonate a local actor, which is the acceptance criterion "free-form actor fields are
not trusted as identity".

**Cursors become opaque strings, but stay clock-free.** On the wire a coordination cursor is
an opaque string that the client must not parse or do arithmetic on. Internally it encodes only
the `coordination_events.sequence` — an `AUTOINCREMENT` integer that is already monotonic
per-database.

This deliberately does **not** copy the `{rfc3339}|{rowid}` shape used by `/v1/changes`. That
format carries a timestamp because `/v1/changes` supports a `since` parameter and therefore
needs time ordering. `/v1/coordination/events` has no `since`, so a timestamp would buy
nothing and would make the feed depend on a clock — the opposite of the acceptance criterion
"per-origin cursors support restart-safe delivery without relying on synchronized clocks." A
bare sequence depends on no clock at all, which satisfies that criterion more directly than
mirroring the existing format would.

Cursors are **per-origin**: a sequence from palace A is meaningless in palace B, so they are
never compared or merge-sorted across palaces. Each remote gets only its own cursor, exactly as
`changes_fanout` already does with its per-remote `BTreeMap`.

`CoordinationCursor` therefore keeps its `i64` payload; only the wire and MCP representation
becomes an opaque string. For compatibility with the tools shipped in Phase 1, the MCP tool
also accepts a bare integer.

**Lease clocks belong to the owning origin.** Expiry is always evaluated by the palace that
owns the task, using its own clock. No caller timestamp is trusted, so nothing depends on
synchronised clocks.

## Stage 4 — Client and routing

**Status: resolved, with deviations recorded below.** Implemented on
`claude/phase3-stage4-coordination-routing`, stacked on Stages 1–3.

`RemoteApi` gains the coordination methods **with default bodies** returning a
`RemoteError::RemoteRejected`-style "unsupported" error. The trait has no default bodies today,
so every added method breaks all four implementors (`RemoteClient` plus three test mocks);
defaults avoid that and let a client talk to a pre-Phase-3 server cleanly. `RemoteClient` gates
on the cached `capabilities` list from the `/v1/info` handshake — that cache is already
populated on every call path, so the check is free.

`resolve_coordination_route(federation, wing)` is added to `mempalace-config`, keyed on the
task's wing. It differs from `resolve_route` in two ways:

- `WriteTarget::Both` is rejected at config load with a hard error, per the no-multi-master
  non-goal.
- The diary hard-override still applies. `wing_agents` coordination stays local
  unconditionally, matching how the diary is protected everywhere else.

`FederationRouter` gains coordination methods. Behaviour by mode:

- `local` — unchanged, no network.
- `remote` — every operation goes to the configured remote; failure is a hard error, matching
  how drawer writes already behave.
- `combined` — exact-ID reads try local first, then each remote in name order, first hit wins;
  the returned value carries `origin`. Writes go to the origin that owns the task. Event reads
  fan out with a per-remote cursor map and the `{unreachable, error}` isolation contract from
  `changes_fanout`, so one down remote never poisons a healthy one.

**Conflict semantics.** A revision conflict from a remote returns that remote's actual revision
verbatim, in the existing `revision_conflict_payload` shape. Retry is the caller's decision;
MemPalace does not retry a conflicting write on the caller's behalf. Storage currently reports
stale revisions inconsistently — `coordination.rs` returns `Err(Invariant("stale revision"))`
while `delegation.rs` and `skills.rs` return `RevisionedWrite::Conflict`. Stage 4 reconciles
coordination onto `RevisionedWrite` so one wire shape can carry it.

The 30 coordination, skill and delegation MCP dispatch arms become `async`. `ToolName::routing()`
gains coordination categories and — unlike today, where it is dead code called only from a test
— is actually consulted by `dispatch_tool`.

### Deviations from this design

1. **`resolve_coordination_route` reads a separate `federation.coordination` table, not
   `federation.wings`.** This document did not specify which table the function reads from. Reusing
   `federation.wings` would mean the `WriteTarget::Both` rejection retroactively broke any existing
   config that legitimately dual-writes drawers for a wing (a documented, encouraged pattern — see
   the combined-wing team workflow in `Federation.md`), the moment that wing also carried coordination
   traffic. A dedicated `federation.coordination` table, structurally identical to `wings`, lets an
   operator configure drawer dual-write and coordination routing for the same wing independently,
   and lets the `write: both` rejection be enforced once, at config load, without runtime knowledge
   of which wings will ever host a task. See `crates/mempalace-config/src/federation.rs`.

2. **`resolve_coordination_route` only applies to `mempalace_task_create`.** Every other
   coordination operation — get/claim/renew/transition, message send/get/ack, artifact/result
   put/get — is keyed by an existing record's ID with no wing in the request at all (none of
   `mempalace_task_claim`, `mempalace_message_get`, etc. take a `wing` argument, matching the local
   tool surface unchanged since Phase 1). There is therefore no wing to resolve a route against for
   those calls. They use a **local-first, ID-discovery fallback** instead: local storage first, then
   — whenever any remote is configured, independent of any specific wing's resolved mode — each
   remote in name order, exactly mirroring `DeleteDrawer`'s existing "no cross-palace ID mapping"
   reasoning. "Writes go to the origin that owns the task" (as originally written above) describes
   this discovery process, not a wing-routed decision. See the `FederationRouter` "Coordination"
   section comment in `crates/mempalace-mcp/src/federation.rs`.

3. **The typed conflict crosses the wire as a new `mempalace_remote::RemoteRevisionedWrite<T>`,
   not `mempalace_storage::RevisionedWrite<T>`.** `mempalace-remote` deliberately has no dependency
   on `mempalace-storage` (it is meant to stay a lightweight HTTP client), so it cannot re-export the
   storage-layer enum. `RemoteRevisionedWrite` mirrors it in shape; `RemoteClient::execute_revisioned`
   parses a `409` body's `code`/`actual_revision` fields directly (bypassing the generic
   `RemoteRejected` error path, which would otherwise flatten them into an opaque string) so the
   revision survives the trip back to the router untouched.

4. **`mempalace_task_claim`/`mempalace_task_renew`/`mempalace_task_transition`'s success shape
   changed.** Before this stage a successful claim/renew/transition returned the bare task object,
   and a stale-revision conflict was a JSON-RPC `InternalError` the caller had to catch. Reconciling
   coordination onto `RevisionedWrite` made it natural — and consistent with
   `mempalace_skill_promote`/`mempalace_delegation_span_close`, which already do this — to give these
   three tools the same `{"success": true, "task": {...}}` / `{"success": false, "conflict": {...}}`
   shape instead. `docs/Coordination.md` does not document exact response shapes per tool, so this
   is not a documented-behaviour break, but it is a real one for any existing caller that expected a
   bare task object or an MCP-protocol-level error on conflict.

5. **The capability-gate error is a new `RemoteError::CapabilityMissing` variant, not a
   `RemoteRejected`-shaped 404.** The design text called for "a clear, non-degradable error naming
   the remote, not a confusing 404"; reusing `RemoteRejected` with a synthetic 404 status would have
   produced exactly the confusable shape the design was trying to avoid, so a dedicated variant
   carries the remote name and the missing capability string instead.

## Stage 5 — A2A adapter

A new crate, `mempalace-a2a`, depending on `mempalace-storage` and `mempalace-federation` and
nothing else. It maps discovery and the task/message/artifact lifecycle:

- **Agent Card** ← palace identity, configured wings, and the capability list.
- **A2A Task** ↔ `coordination_tasks`, per the state table below.
- **A2A Message** ↔ `coordination_messages`.
- **A2A Artifact** ↔ `coordination_artifacts`.

A2A reached v1.0 in 2026 under the Linux Foundation and defines **nine** task states against
MemPalace's seven, so the mapping is not bijective in either direction:

| A2A | MemPalace | Note |
|---|---|---|
| `SUBMITTED` | `Pending` | |
| `WORKING` | `Running` | |
| `INPUT_REQUIRED` | `InputRequired` | |
| `COMPLETED` | `Completed` | |
| `FAILED` | `Failed` | |
| `CANCELED` | `Cancelled` | note the spelling difference — A2A uses one `l` |
| `AUTH_REQUIRED` | `InputRequired` | coerced; both mean "interrupted, awaiting external input" |
| `REJECTED` | `Failed` | coerced; terminal and unsuccessful |
| `UNSPECIFIED` | — | rejected as malformed; never coerced |
| — | `Expired` | outbound only, emitted as `FAILED` |

**This revises the original rule.** Requiring a mapping that is total in both directions is not
achievable without either adding `AuthRequired`, `Rejected` and an `Expired` analogue to the
core `TaskState` — which is exactly the "adapters must not dictate the core schema" non-goal —
or rejecting well-formed A2A messages that carry a legal state. Neither is acceptable.

The rule is therefore: coercion is permitted where it is **documented, deterministic, and
lossless in audit**. The inbound envelope is already stored verbatim as a
`role = "protocol_envelope"` artifact, so the original state is always recoverable and the
coercion is always visible. What remains forbidden is *silent* coercion — discarding the
original with no record, or coercing a state not named in the table above.

**Isolation rule.** No A2A field may become a column. Fields with no internal home are
preserved by storing the inbound envelope as an immutable artifact with
`role = "protocol_envelope"` and `media_type = "application/vnd.a2a+json"`, referenced from the
task. This reuses the artifact mechanism that already exists, keeps the core schema untouched,
and keeps the exchange auditable — which is the acceptance criterion "A2A lifecycle mappings
preserve internal semantics and auditability" without the criterion "adapter-specific fields do
not leak into the core storage schema" being violated.

## Stage 6 — MCP Tasks adapter

The MCP Tasks extension left experimental core and became the official
`io.modelcontextprotocol/tasks` extension in the 2026-07-28 specification. That satisfies issue
#102's "once its wire model is stable" condition. Implement against the published extension
specification — read it, do not infer it from the core protocol.

It defines three methods — `tasks/get`, `tasks/update`, `tasks/cancel` — and a task carrying
`taskId`, `status`, `statusMessage`, `createdAt`, `lastUpdatedAt`, `ttlMs`, `pollIntervalMs`,
`result`, `error`, and an `inputRequests` map. A missing client capability is signalled with
JSON-RPC error `-32003`.

Its status set is smaller than either of the other two — five values, only one of them
non-terminal:

| MCP Tasks | MemPalace | Note |
|---|---|---|
| `working` | `Running` | |
| `input_required` | `InputRequired` | |
| `completed` | `Completed` | |
| `failed` | `Failed` | |
| `cancelled` | `Cancelled` | two `l`s here, unlike A2A |
| — | `Pending` | outbound only; emitted as `working`, since MCP has no queued state |
| — | `Expired` | outbound only; emitted as `failed` |

Inbound is total, so no coercion is needed in that direction. Outbound coerces `Pending` and
`Expired`, under the same documented-deterministic-auditable rule as Stage 5.

`ttlMs` maps onto the task's existing `expires_at`, and `pollIntervalMs` is adapter policy, not
stored state — neither becomes a column. Same isolation rule and same envelope-as-artifact
mechanism as Stage 5.

## Documentation

Per [CLAUDE.md](../CLAUDE.md), docs ship in the same PR as the behaviour, not after:

- `docs/Coordination.md` — the "local-only and is not federated" sentence is now wrong.
- `docs/Federation.md` — coordination routes in the REST table (§1.4), a new authorization
  section, a new part on federated coordination, and a troubleshooting entry per new error
  string.
- `docs/Config-Schema.md` — `scopes` under the server config, the coordination route rule under
  federation config, and the `WriteTarget::Both` rejection in the validation-errors list.
- `docs/Release-Scope.md` — the tool count, and the sentence asserting the coordination tools
  are local-only.
- `docs/Operator-Standard.md` — token scoping in federation server deployment, and split-palace
  state in storage recovery.
- `README.md` — tool count in two places, and the crates table if a crate is added.
- `CLAUDE.md` — package list if a crate is added.
- `.github/workflows/ci.yml` — a new crate must join the `remaining-package-tests` matrix, or it
  is built but never tested.

Two doc/code drifts should be corrected while in these files: `Config-Schema.md` describes the
route precedence with the diary override applied last, when the code short-circuits on it
first; and it omits that plain `http://` is rejected for non-loopback hosts, which is a hard
config-load failure.

## Open questions

1. Should `wing_unscoped` tasks be federatable at all, or refused until re-homed to a real wing?
2. Should `coordination_claim` imply `coordination_write`, or must a worker token carry both?
3. Does the A2A adapter need its own HTTP surface, or is it a translation layer over the
   existing `/v1/coordination/*` routes?
