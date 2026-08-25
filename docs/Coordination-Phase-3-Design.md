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
   — independent of any specific wing's resolved mode — each remote in name order, exactly
   mirroring `DeleteDrawer`'s existing "no cross-palace ID mapping" reasoning. "Writes go to the
   origin that owns the task" (as originally written above) describes this discovery process, not
   a wing-routed decision. See the `FederationRouter` "Coordination" section comment in
   `crates/mempalace-mcp/src/federation.rs`.
   
   This is independent of *which* wing a record turns out to belong to, but it is **not**
   independent of whether coordination federation was configured at all. An initial version of
   this fallback ran whenever *any* remote was configured, for *any* reason — a palace federating
   drawers only, with no `federation.coordination` entry, would still send a local coordination
   miss to that remote. `FederationRouter::coordination_federation_enabled()` closes that: the
   fallback now short-circuits to a local-only result unless `federation.coordination` has at
   least one entry or `default_mode` (which `resolve_coordination_route` itself falls through to
   for any wing without an explicit entry) is non-`Local`. See
   `coordination_fallback_records_zero_remote_calls_without_coordination_federation_config` in
   `crates/mempalace-mcp/src/federation.rs`.

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

6. **Post-implementation fix: `resolve_coordination_route` was keyed on the raw, un-normalised
   wing string, not the canonical form.** `tool_task_create` called `resolve_coordination_route`
   with `input.wing` straight off the wire, before `WingId::normalized` ran (normalisation
   happened only later, inside the local store's own write path, and not at all on the branch
   that routes remote). Every other routing path in the palace (`tool_add_drawer` → `parse_wing_id`
   → `resolve_drawer_route`) normalises before routing; coordination's wing-carrying tool did not.
   Two concrete failures followed: a short/mixed-case spelling of `wing_agents` (`"agents"`,
   `"Wing_Agents"`) did not `==` the literal compared against in the diary hard-override, so it
   fell through to `default_mode` and could route the diary wing to a configured remote before the
   server-side check in [Wing is the authorization key](Federation.md#wing-is-the-authorization-key)
   ever saw it; and a short-form spelling of an operator's explicit `federation.coordination["wing_x"]:
   { mode: local }` pin missed the map lookup for the same reason, silently persisting the task
   remotely instead of honoring the pin. Fixed in three layers: `tool_task_create` now normalises
   the wing once via `parse_wing_id` and uses that canonical value for both the route decision and
   the outgoing request (local or remote); `resolve_coordination_route` normalises defensively a
   second time and fails closed to `local_rule()` — not `default_mode` — on a wing it cannot
   normalise at all, since a routing decision that gates data egress must refuse to guess; and
   `resolve_federation_config` now rejects a non-canonical `federation.coordination` key at load
   rather than silently normalising it, because normalising the key would retroactively activate a
   rule that is inert in every config written before this fix. See
   `crates/mempalace-mcp/src/lib.rs` (`tool_task_create`), `crates/mempalace-config/src/federation.rs`
   (`resolve_coordination_route`, `resolve_federation_config`), and the corresponding tests in both
   files.

7. **Post-implementation fix: `GET /v1/coordination/events` and `GET /v1/coordination/inbox`
   computed `next_cursor` over unfiltered rows, leaking cross-wing existence and volume.** Both
   feeds filtered to the caller's visible wings (and excluded the shared diary wing,
   `wing_agents`) only *after* storage had already applied its `LIMIT`/cursor boundary to the
   unfiltered row set. A caller could not see an invisible row in the response, but the
   *cursor* was still derived from it: an explicit `?wing=` naming a wing the token cannot see
   returned `next_cursor: null` when that wing had no rows, and a real sequence number when it
   had two or more — deterministically distinguishable in one request, with no unguessable ID
   involved, since wing names are operator-chosen. The same shape applied to
   `GET /v1/coordination/inbox`'s unfiltered branch against a guessed `recipient` string. This is
   the same failure class deviation 6 and `4cac227` already closed elsewhere on this branch
   (`parent_id`/`dependencies` in task creation): an existence oracle through a channel that
   looks unrelated to the actual response body.

   Fixed by pushing wing visibility (and the diary exclusion) into the storage query itself,
   mirroring `route_drawers_list`'s use of `DrawerFilter::wings`: `CoordinationStore::events` and
   `CoordinationStore::inbox` now take a `CoordinationVisibility` parameter applied to the SQL
   predicate before the `LIMIT`/cursor boundary is computed, so an invisible row can never
   influence `next_cursor`. `CoordinationVisibility` has no "empty means unconstrained" footgun
   the way `DrawerFilter::wings` does: it is `Trusted` (no restriction at all, including the
   diary wing — reserved for the local MCP surface, which has no HTTP identity to scope against)
   or `Federated(Option<&[String]>)`, where `Federated(Some(&[]))` is an explicit, handled
   "nothing is visible" and the diary wing is always excluded regardless of what the restriction
   list contains. Pushing filtering into the query also let the inbox route's over-fetch/post-filter
   loop (added for a different bug — a client silently skipping a visible message when the loop
   stopped early, tracked by `last_examined_sequence`) be deleted entirely: storage's own
   `has_more`/cursor boundary is now computed over the already-filtered set, so "examined
   everything" and "storage's page boundary" always agree.
   `coordination_inbox_cursor_does_not_skip_the_second_visible_message` still exercises that
   at-least-once-delivery property and passes unchanged. See
   `crates/mempalace-storage/src/coordination.rs` (`CoordinationVisibility`, `events`, `inbox`)
   and `crates/mempalace-server/src/lib.rs` (`route_coordination_events`,
   `route_coordination_inbox`).

8. **Post-implementation fix: `FederationRouter::coordination_inbox_fanout`/
   `coordination_events_fanout` had neither the coordination opt-in gate nor the diary
   hard-override.** Deviations 2 and 6 above closed the same two invariants — "coordination
   never federates unless `federation.coordination`/`default_mode` says so" and "`wing_agents`
   coordination never federates, unconditionally" — but only on the paths that call
   `resolve_coordination_route` or the ID-discovery fallback (`coordination_read_fallback`/
   `coordination_write_fallback`/`coordination_task_revisioned_fallback`). The two aggregate
   fan-out methods behind `mempalace_inbox_read` and `mempalace_coordination_events` are neither:
   they are unconditional concurrent broadcasts to every configured remote, gated at their call
   sites in `crates/mempalace-mcp/src/lib.rs` only by `FederationRouter::has_remotes()` — true
   whenever *any* remote is configured for *anything* (drawers, KG, anything), not specifically
   coordination. A palace federating drawers only, with an empty `federation.coordination` table
   and `default_mode: local`, still sent the recipient name and any wing filter to every
   configured remote on every `mempalace_inbox_read`/`mempalace_coordination_events` call — and a
   caller requesting `wing: "wing_agents"` (or a non-canonical spelling of it) got that filter
   forwarded to every remote with no diary check at all, since neither fan-out method ever calls
   `resolve_coordination_route` or checks `SHARED_AGENT_DIARY_WING`.

   Fixed by adding both guards directly inside `coordination_inbox_fanout` and
   `coordination_events_fanout` themselves, rather than at their two call sites, so a future
   aggregate-read call site cannot add a fan-out without inheriting the checks: each method now
   returns an empty `BTreeMap` (the local page still returns normally, just with no
   `remote_messages`/`remote_events` entries — indistinguishable from a healthy config with zero
   configured remotes) when `coordination_federation_enabled()` is false, or when the requested
   `wing` normalises (via `WingId::normalized`, so short/mixed-case/whitespace spellings all
   count, and a wing that fails to normalise at all fails CLOSED the same way) to
   `SHARED_AGENT_DIARY_WING`. A request with no `wing` filter is unaffected. See
   `wing_blocks_coordination_fanout` and `coordination_events_fanout`/`coordination_inbox_fanout`
   in `crates/mempalace-mcp/src/federation.rs`.

9. **Post-implementation fix: `coordination_read_fallback` treated every remote error as a
   skippable miss, including `Unauthorized`/`VersionSkew`/`CapabilityMissing` and a malformed
   response.** The ID-discovery read fallbacks (`tool_task_get`, `tool_message_get`,
   `tool_artifact_get`, `tool_result_get`) logged and moved on to the next remote for *any*
   error, not just a `404`. A misconfigured token, an incompatible remote, or a remote that does
   not support coordination at all was therefore indistinguishable from "the record does not
   exist" — a caller received `{"found": false}` either way, with no way to tell a genuine miss
   apart from a broken remote.

   Fixed by narrowing the "try the next remote" set for reads to a `404`-shaped `RemoteRejected`
   and a genuinely degradable `Unreachable` remote (the federation-wide "reads degrade" rule);
   every other error — `Unauthorized`, `VersionSkew`, `CapabilityMissing`, `InvalidResponse`, or
   any other `RemoteRejected` status — now surfaces as a hard `ToolError`, matching the error
   construction every other federation read path in `crates/mempalace-mcp/src/federation.rs`
   already uses. `coordination_task_get_fallback`/`coordination_message_get_fallback`/
   `coordination_artifact_get_fallback`/`coordination_result_get_fallback` changed signature from
   `Option<Value>` to `ToolResult<Option<Value>>` to carry the new error path; their callers in
   `crates/mempalace-mcp/src/lib.rs` now propagate it with `?`. See `coordination_read_fallback`
   and its error-policy doc comment, and the `coordination_read_fallback_surfaces_*` tests, in
   `crates/mempalace-mcp/src/federation.rs`.

10. **Post-implementation fix: the ID-discovery write fallback (`coordination_write_fallback`
    and the claim/renew/transition equivalents) iterated every configured remote, not just ones
    wired up for coordination, and treated `CapabilityMissing` as terminal.** A remote configured
    only for drawer or KG federation — never referenced by any `federation.coordination` rule —
    still received every local coordination write miss, in `BTreeMap` name order. If that
    unrelated remote's name sorted before the actual coordination remote's name and it does not
    advertise the `coordination` capability (e.g. a pre-Stage-4 server, or one simply never
    intended for coordination traffic), the resulting `CapabilityMissing` was treated the same as
    any other non-404 write error: terminal, ending the search before the real owning remote was
    ever tried.

    Fixed in two parts. First, `FederationRouter::coordination_candidate_remotes` computes the
    set of remote names actually referenced by `federation.coordination` (across every wing —
    there is no wing to key a single lookup by, so the *union* across all configured coordination
    rules is used) plus `default_remote` when `default_mode` is non-`Local`; `coordination_read_fallback`,
    `coordination_write_fallback`, and `coordination_task_revisioned_fallback` (see deviation 11
    below) all iterate only that candidate set instead of every configured remote. Second,
    `CapabilityMissing` from a remote still inside the candidate set is treated the same as a
    `404` for a *write* — "not this palace" — because the capability list comes live from the
    remote's own `/v1/info`, independent of what `federation.coordination` says about it, so a
    candidate can genuinely turn out not to run coordination at all. This is the opposite of
    deviation 9's read policy on the very same error, deliberately: a write cannot afford to
    guess past a remote it could not get an answer from (wrong guess risks a second, divergent
    record for the same task on the wrong palace), so every other error, including an unreachable
    remote, stays terminal for writes. See `coordination_write_fallback`'s doc comment and the
    `coordination_write_fallback_reaches_later_remote_past_earlier_capability_missing` test
    (two remotes, the non-coordination one sorting first) in
    `crates/mempalace-mcp/src/federation.rs`.

11. **Post-implementation fix: the remote claim/renew/transition envelope flattened the task DTO
    at the top level instead of nesting it under `task`.** Deviation 4 above documents the local
    success shape as `{"success": true, "task": {...}}`. The remote fallback paths
    (`coordination_task_revisioned_fallback`, and `coordination_task_transition_fallback` before
    it was merged into that same function) instead serialised the task DTO to the top level of
    the response object and added `success`/`applied_to` beside its fields — e.g.
    `{"task_id": ..., "success": true, "applied_to": "remote:hub"}` — with no `task` key at all.
    A client that deserialises against the documented local shape silently loses the task the
    moment the same operation falls back to a remote.

    **This is a breaking change for any existing caller written against the flattened remote
    shape** — call it out in the PR description. Fixed by building the same
    `{"success": true, "task": {...}, "applied_to": "remote:<name>"}` envelope on both paths;
    `applied_to` stays a sibling of `task` (envelope-level metadata about *how* the write was
    served, not a property of the task record itself), matching where every other remote
    annotation in this file (`origin`, `applied_to`) already lives — at the envelope/object level
    the caller inspects first, not nested inside the payload it annotates. The revision-conflict
    branch was not affected: it already called the shared `revision_conflict_payload` helper on
    both local and remote paths, so local and remote conflicts have always agreed. See
    `coordination_task_revisioned_fallback` and the `coordination_task_write_fallbacks_nest_task_under_task_key`/
    `coordination_task_write_local_and_remote_envelopes_match` tests in
    `crates/mempalace-mcp/src/federation.rs`.

12. **Post-implementation fix: `POST /v1/coordination/messages/{id}/ack` ran its claimed
    `actor` through the same identity-prefixing rule as every other actor-shaped field, which
    made a federated acknowledgement fail whenever the remote token's identity differed from
    the message's `recipient`.** `route_coordination_message_send` stores `recipient` verbatim
    — deviation 3's "Actor identity" note is explicit that `recipient` is not identity-derived,
    since it addresses a message rather than asserting who the caller is. But
    `route_coordination_message_ack` derived its `actor` the same way `created_by`/`sender`/
    `worker` are derived: `{identity}:{claimed}` whenever the claim differed from the
    authenticated token's identity. Storage's `acknowledge_message` requires the final actor to
    equal `recipient` **exactly** (`ONLY_RECIPIENT_MAY_ACKNOWLEDGE`). Since a prefixed string
    can never equal an unprefixed one, the only way the old code could succeed was for the
    token's own identity to already equal the recipient — which defeats the entire point of
    routing an acknowledgement through a hub or a token whose configured name is not the
    recipient's own name, the ordinary shape of a multi-agent deployment.

    Fixed by `resolve_ack_actor`, used only on the ack route: when the claimed actor exactly
    equals the message's `recipient`, it is used as-is (proving the caller knows the
    (unauthenticated) address the message was already sent to — no stronger a claim than the
    sender who chose that address); any other claim — including one that happens to equal the
    caller's own token identity but not the recipient — still goes through
    `resolve_coordination_actor`'s ordinary prefixing, so a claim naming an unrelated identity
    can never be recorded bare. This preserves the impersonation protection
    `resolve_coordination_actor` exists for: the *only* new thing a caller can achieve is
    acknowledging with the literal recipient string, which was already knowable from the
    message itself (or from having addressed it), not from breaking any identity boundary. See
    `resolve_ack_actor` and the module-level "Actor identity" note in
    `crates/mempalace-server/src/lib.rs`, and
    `coordination_message_ack_succeeds_when_claim_matches_recipient_not_identity`/
    `coordination_message_ack_still_rejects_a_claim_impersonating_someone_else` in the same
    file's test module.

13. **Post-implementation fix: `mempalace_inbox_read`/`mempalace_coordination_events`'s
    `input_schema` never declared `remote_cursors`, even though both tools' implementations
    already read it (`parse_cursors_arg(arguments, "remote_cursors")`).** A schema-driven MCP
    client has no way to send an argument its declared schema does not mention, so every
    federated page restarted each remote's paging from the beginning — the field was
    functional but undiscoverable.

    Fixed by adding `remote_cursors` (an object of string values, matching what
    `parse_cursors_arg` actually parses: `{"<remote_name>": "<opaque_cursor>"}`) to both tools'
    `input_schema`, and describing it — and the corresponding `remote_messages`/`remote_events`
    response fields, including that they are empty objects (not an error) whenever coordination
    federation is not configured for the requested wing — in both tools' `description`. See
    `ToolName::definition` for `InboxRead`/`CoordinationEvents` in
    `crates/mempalace-mcp/src/lib.rs`, the schema assertions in
    `inbox_read_and_coordination_events_declare_remote_cursors_schema`, and the end-to-end
    pagination proofs `coordination_events_remote_cursors_round_trip_paginates_without_repeats`/
    `coordination_inbox_remote_cursors_round_trip_paginates_without_repeats` in
    `crates/mempalace-mcp/tests/federation_e2e.rs`.

14. **Post-implementation fix: `mempalace_inbox_read`/`mempalace_coordination_events`'s
    aggregate fan-outs still queried every configured remote, not just the ones actually named
    by a `federation.coordination` rule.** `coordination_events_fanout` and
    `coordination_inbox_fanout` correctly gated on `coordination_federation_enabled()` and the
    `wing_agents` diary suppression (deviations already covering that), but their loop bodies
    still iterated `&self.remotes` unfiltered — unlike the three ID-discovery fallbacks
    (`coordination_read_fallback`, `coordination_write_fallback`,
    `coordination_task_revisioned_fallback`), which already narrow to
    `coordination_candidate_remotes()`. Once any wing had a coordination rule, both aggregate
    tools sent the recipient/wing/task_id filter to every configured remote, including ones
    wired up only for drawer or KG federation and never referenced by coordination configuration
    at all — such a remote's correct `CapabilityMissing` decline was then recorded as
    `{"unreachable": true, ...}`, misreporting "never configured for this" as "currently down".

    This is the third time this exact seam — the candidate-narrowing fallbacks and the
    aggregate fan-outs living as parallel, hand-synchronised code paths in
    `crates/mempalace-mcp/src/federation.rs` — was fixed on one family and missed on the other:
    deviation-adjacent fix `bd7cd21` added the opt-in/diary gate to the fan-outs after it shipped
    on the fallbacks first, then a later fix added the candidate-set narrowing to the three
    fallbacks while leaving both fan-outs iterating every configured remote.

    Fixed two ways. First, both fan-out loops now iterate `coordination_candidates()`, a single
    helper method that yields `self.remotes` filtered to `coordination_candidate_remotes()` — the
    same set the three fallbacks already used via a hand-copied filter, now backed by one
    implementation all five loops share, so a future change to the candidate rule (or a fix to a
    bug in it) reaches every call site at once. This does not stop a *sixth*, not-yet-written
    aggregate-read call site from re-introducing unfiltered iteration; the mitigation is that
    there is now exactly one place to update, not five, when adding one. Second, a
    `CapabilityMissing` remote is now reported as `{"capability_missing": true, "capability":
    "...", "error": "..."}`, distinguishable from a genuinely unreachable remote's unchanged
    `{"unreachable": true, "error": "..."}` — see the `CoordinationFanoutFailure` type in
    `crates/mempalace-mcp/src/federation.rs`. See
    `inbox_read_and_coordination_events_fanout_only_contact_the_coordination_candidate`/
    `coordination_fanout_distinguishes_capability_missing_from_unreachable` in
    `crates/mempalace-mcp/src/lib.rs`, and [docs/Federation.md](Federation.md)'s aggregate
    fan-out section.

15. **Post-implementation fix: `is_local_record_missing` (the federation-fallback gate for
    `mempalace_task_claim`/`_renew`/`_transition`, `mempalace_message_send`/`_acknowledge`, and
    `mempalace_artifact_put`/`mempalace_result_put`) matched a bare `"not found"` string
    literal, not a pinned constant.** Every other `Invariant` message a federated coordination
    write can hit (`LEASE_HELD_BY_ANOTHER_WORKER`, `ONLY_RECIPIENT_MAY_ACKNOWLEDGE`,
    `STALE_REVISION_PREFIX`-adjacent constants, etc.) is a `pub const` in
    `crates/mempalace-storage/src/coordination.rs` specifically so that rewording the message is
    a compile error at its construction site, per that module's doc comment. The task/message
    "not found" messages `require_task`, `task_wing`, and `acknowledge_message`'s own message
    lookup produce had no such constant — a future rewording of any of them would have silently
    disabled federation fallback for that path (or, on the server side, silently reclassified a
    404 as something else) with no compiler signal.

    Fixed by adding `pub const NOT_FOUND_SUFFIX: &str = " not found"` next to the other pinned
    fragments, rebuilding all three construction sites (`crates/mempalace-storage/src/coordination.rs:772,1084,1091`)
    from it, and matching against it — instead of the literal — in both
    `mempalace-mcp`'s `is_local_record_missing` and `mempalace-server`'s
    `coordination_storage_error`. `conflict_error_messages_start_with_their_pinned_constants` (in
    `crates/mempalace-storage/src/coordination.rs`) was extended to drive a genuinely-missing
    task and message through the real storage calls and assert the resulting message ends with
    the constant, alongside `is_local_record_missing_matches_real_missing_task_and_message_errors`
    in `crates/mempalace-mcp/src/lib.rs`, which does the same at the MCP layer. Artifacts and
    results were checked separately: `get_artifact`/`get_result` return `Option`, not an
    `Invariant`-shaped "not found" at all, so they were never affected by this predicate; the
    only other "not found" `Invariant` producers in `mempalace-storage`
    (`delegation.rs`, `skills.rs`) belong to unrelated subsystems `is_local_record_missing` never
    sees.

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
