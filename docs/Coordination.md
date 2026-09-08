# Native local coordination

MemPalace stores durable coordination state in the palace's local `storage.sqlite3`. It provides persistence and concurrency control; the host agent runtime still owns worker spawning, scheduling, tool execution, and live budget enforcement. Federation is opt-in and, as of issue #102 Stage 4, extends all the way to the MCP tool surface on this page: `mempalace-server` exposes tasks, messages, artifacts, results, and audit events over `/v1/coordination/*` to a caller holding the right scoped token, under the same wing-scoped authorization as the rest of the federation REST surface, and `mempalace-mcp`'s `RemoteApi`/`FederationRouter` route the MCP tools below to a configured remote when a wing's `federation.coordination` rule (or, for the ID-keyed tools, the mere presence of a configured remote) calls for it. The CLI is unaffected — it has never read or written coordination state. See [Federation → Part 7, Federated coordination](Federation.md#part-7--federated-coordination) for the full routing and wire behaviour.

## Data and guarantees

- Tasks have immutable IDs, parent and dependency references, optional budget metadata and expiry, a lifecycle state, an owner, a lease expiry, and a monotonically increasing revision.
- Messages are append-only, addressed to one recipient, ordered by a local sequence, and explicitly acknowledged.
- Results are immutable structured payloads with stable IDs. Artifacts are immutable, carry a role and media type, and include a BLAKE3 content hash.
- Every mutation appends an audit event in the same SQLite transaction. Events identify the actor and task transition and are read with an opaque sequence cursor.
- Task creation, message send, result, and artifact writes deduplicate only on `(actor, idempotency_key)`. Replaying a key returns the committed record. Similar content with different keys remains distinct.
- Exact get operations query primary IDs only and return `found: false` on a miss. They never invoke semantic search.
- Claims, renewals, and transitions use an expected revision. A stale revision fails explicitly. A valid lease excludes other workers; an expired lease can be reclaimed without deleting prior events.
- Every task belongs to a wing, normalised the same way project wings are normalised everywhere else in the palace, so `myproject` and `wing_myproject` resolve to the same wing. Messages, artifacts, and results carry no wing column of their own and reach it through their mandatory task reference; audit events do carry their own `wing` column, always the owning task's wing materialised inside the same transaction as the mutation, never a value supplied by a caller.
- `wing_unscoped` is a reserved wing name meaning "created before wings existed." The operational schema upgrades itself in place the first time a palace opens under this code — existing tasks, messages, artifacts, results, and events are untouched and read back with that wing, with no separate migration step or data export/import. Task creation rejects `wing_unscoped` (and its unprefixed spelling `unscoped`, which normalises to it) — the reserved name identifies migrated rows, and a new task cannot be created there.
- `mempalace_coordination_events` and `mempalace_inbox_read` accept an optional wing filter, normalised the same way as task creation, so a filter of `myproject` matches records stored under `wing_myproject`. Omitting the filter is unscoped and spans every wing, matching how visibility already worked before wings existed.
- `wing_agents` — the shared agent diary wing — never federates, regardless of token scope. A remote `POST /v1/coordination/tasks` targeting it fails with 422; every other coordination route, and the inbox and event feeds, treat `wing_agents` state as though it does not exist. This is the same diary hard-override applied everywhere else in the palace; see [Federation → Part 7, Federated coordination](Federation.md#part-7--federated-coordination).

Delivery is at least once with idempotent writes. MemPalace does not promise exactly-once task execution.

Task states are `pending`, `running`, `input_required`, `completed`, `cancelled`, `failed`, and `expired`. Terminal states cannot transition again. Only a current owner may transition owned work, except that another actor may request cancellation. An acknowledgement must name the message's addressed recipient.

Actor IDs are asserted by the local host runtime. MemPalace enforces ownership and recipient checks against those IDs; transport-level authentication and worker execution remain host-runtime responsibilities.

**Acknowledgement is scoped to the wing, not to the acknowledging agent.** A recipient is a free-form string the sender chooses; it is not an authenticated identity and nothing verifies that the agent acknowledging a message is the agent it was addressed to. The check is that the acknowledgement names the recipient the message was stored with — so any caller who can reach the message can satisfy it. Locally that is any agent on the palace; over federation it is any token holding `coordination_write` on that message's wing. The wing is the authorization boundary here, and it is the same boundary `mempalace_inbox_read` already uses, which accepts any `recipient` argument rather than binding to the caller. Do not treat an acknowledgement as proof that a particular agent saw a message.

Task titles, descriptions, JSON payloads and budgets, and artifact content are limited to 1 MiB. Idempotency keys are limited to 256 bytes. Inbox and event cursors are `null` when a page contains the final available records; a non-null cursor indicates that another page is available.

## MCP tools

The native local tool surface is:

- `mempalace_task_create` (requires `wing`), `mempalace_task_list`, `mempalace_task_get`, `mempalace_task_claim`, `mempalace_task_renew`, `mempalace_task_transition`
- `mempalace_message_send`, `mempalace_message_get`, `mempalace_message_acknowledge`, `mempalace_inbox_read` (takes an optional `wing` filter)
- `mempalace_artifact_put`, `mempalace_artifact_get`
- `mempalace_result_put`, `mempalace_result_get`
- `mempalace_coordination_event_get`, `mempalace_coordination_events` (takes an optional `wing` filter)

Treat returned cursors as opaque and persist them with worker state. After restart, retrieve known task, message, result, and artifact IDs directly, then continue the inbox or event stream from the stored cursor.

As of issue #102 Stage 4, this tool surface is federation-aware: `mempalace_task_create` routes by its wing's `federation.coordination` rule, and the other ID-keyed tools above fall back across configured remotes by ID after a local miss (mirroring `mempalace_delete_drawer`'s existing local-first pattern) — a task's `wing` is never supplied to those calls, so there is nothing else to route by. `mempalace_inbox_read`/`mempalace_coordination_events` always read local and additionally fan out to every configured remote, reporting `remote_messages`/`remote_events` alongside the local result — `mempalace_coordination_event_get` is the one exception, staying local-only because Stage 3 never exposed a single-event GET route on the wire. Both fan-out tools also accept a `remote_cursors` object argument (`{"<remote_name>": "<opaque_cursor>"}`) to continue a specific remote's page independently of the local `cursor`; a page's own `remote_messages`/`remote_events` entries carry the `next_cursor` to feed back for that remote. See [Federation → Part 7, Federated coordination](Federation.md#part-7--federated-coordination) for the full routing rules, the server-side REST surface used by a remote peer, and the conflict/capability-gate error shapes.

## Task discovery

`mempalace_task_list` is a `RoutableCoordination` read. It returns a local
`{tasks, next_cursor}` page and, when coordination federation is enabled,
independent pages under `remote_tasks[remote_name]`. It never merges origins.
The matching REST route is `GET /v1/coordination/tasks`; `RemoteApi::coordination_tasks`
requires the additive `coordination_task_list` capability. Older peers return
`capability_missing` in-band; other remote failures return `unreachable` and a
bounded error message. A failure preserves that origin's supplied cursor.

Filters are `wing`, `state`, `owner`, `created_by`, and `parent_id`. `limit` defaults
to 50 and is clamped to 1–200. Results follow immutable insertion sequence, with
no selectable sort. The schema upgrade adds `coordination_tasks.sequence`,
backfills existing tasks by parsed creation time and task ID, and uses an
AUTOINCREMENT allocator plus an insert trigger so deletion or VACUUM cannot
reuse sequence values. Cursors are opaque strings, using the same sequence
encoding as the REST coordination feeds. State and ownership filters observe
current rows, not a snapshot: a task that becomes eligible behind a cursor
requires a new scan to discover.

**Unfiltered REST lists are allowed.** The `require_coordination_read` gate and
`CoordinationReadScope` apply exactly as for events. SQL excludes invisible,
diary and unscoped wings before pagination; explicitly filtering an excluded
wing returns an empty page, not 403. Local MCP reads use trusted visibility.

Each `TaskListItem` contains `task_id`, `wing`, `state`, `revision`, `title`,
`title_truncated`, `owner`, `lease_expires_at`, `parent_id`, `dependency_count`,
`created_by`, `created_at`, and `expires_at`. Title prefixes are limited to 1,024
UTF-8 bytes without splitting a character. Description, budget and dependency
IDs are omitted. A zero dependency count means no declared dependencies; a
positive count does not imply unmet dependencies, and claiming does not check
dependency completion. Lease timestamps identify reclaim candidates; the
owning palace's clock and claim checks decide whether claiming succeeds.

The worker flow is `task_list → choose → task_claim(expected_revision) → execute
the full task returned by claim`. A racing revision produces the normal CAS
conflict. `task_get` remains available for inspection and refreshes.

Task-list budgets are separate from the existing 1 MiB field/payload bounds:

- Local and REST pages cap their encoded JSON at 256 KiB, including escaping and
  the page envelope. REST accepts a smaller `byte_budget` (minimum 128 bytes).
  Pagination reserves cursor space and stops before an item that would exceed
  the budget; the cursor is derived from the last emitted item.
- If one item cannot fit an empty page, the call returns an explicit error.
  Existing unbounded actor strings remain valid and are never truncated or
  silently skipped. Use `task_get` to inspect the task; a larger page budget
  helps only below the 256 KiB ceiling. Resolving a permanently oversized record
  requires a separate data-repair decision. No write-time actor limits or
  automatic migration of identities are introduced here.
- The MCP compact aggregate payload caps at 1 MiB. The router reserves encoded
  remote names, original cursors and error envelopes, then divides the remaining
  budget across selected origins before making requests. Remote pages exceeding
  their allocation are rejected whole, preserving the original cursor. If an
  origin receives less than 128 bytes, it is not contacted and returns
  `budget_exhausted:true` with its original cursor. Excessive origin metadata
  returns an explicit error asking for fewer origins. The actual MCP result
  envelope (which embeds pretty-printed JSON as text) has an additional 4 MiB
  encoded ceiling. These limits stay below the remote client's 10 MiB ceiling.

Pass `remote_cursors:{"work":"<cursor>"}` to resume remote work. For an independent
remote continuation, use `include_local:false, remotes:["work"]`; this preserves
the supplied local cursor and leaves other remotes untouched. Selecting fewer
remotes also gives each one more of the aggregate budget. Omitted `remotes`
means all coordination candidates; `remotes:[]` means no remote reads. Keep
exhausted origins out of subsequent requests: an omitted cursor starts a new
scan, not an implicit continuation.

## Protocol adapter tools (A2A and MCP Tasks)

Issue #102 Stages 6-8 added two pure-translation crates — `mempalace-a2a` (the
[A2A protocol](https://a2a-protocol.org), v1.0) and `mempalace-mcp-tasks` (the
[MCP Tasks extension](https://github.com/modelcontextprotocol/ext-tasks),
`io.modelcontextprotocol/tasks`) — that translate between those wire protocols and the
`coordination_tasks`/`coordination_messages`/`coordination_artifacts` model above. Neither crate
has an MCP tool, an HTTP route, or any storage handle of its own; Stages 9-10 are what wires them
into the MCP tool surface, so a caller can actually reach the translation. There is still no A2A
HTTP surface and no MCP Tasks transport — these tools translate a wire payload the caller already
received or is about to send over its own transport, they do not speak either protocol on the
network themselves.

Both adapters follow the same "translate AND persist" contract: an "import" tool does not just
convert a wire shape into a MemPalace type and hand it back — it also performs the storage write
and, for task imports, records the raw wire JSON verbatim as an immutable `protocol_envelope`-role
artifact. The audit trail exists only because the tool writes it; a caller that instead calls a
bare translation function and does its own storage writes would not automatically produce one.
`a2a_task`/`a2a_message`/`a2a_artifact`/`create_task_result` arguments must be the **exact wire
JSON text**, not a re-serialized object — re-serializing normalises whitespace and key order and
so silently changes the envelope's content hash (its idempotency key). `mempalace-a2a` and
`mempalace-mcp-tasks` share the `protocol_envelope` artifact role but use distinct `media_type`s
and distinct idempotency-key prefixes (`a2a_envelope:`/`mcp_tasks_envelope:`) so their envelope
artifacts for the same task never collide.

**Task imports create directly in the mapped state, not always `Pending`.** A task arriving from
another system may already be `completed`, `cancelled`, or otherwise past `Pending` on that
system, and the ordinary lifecycle (`create_task` then `claim_task`/`transition_task`) cannot
reach most of those states without asserting a worker identity, a lease, and a transition history
that never actually happened here — `claim_task` is the only route into `Running`, and reaching
`Completed`/`Failed`/`InputRequired` from a fresh task requires it first. Fabricating that history
to make an import land in the right state would be strictly worse than the state being wrong: the
audit trail would lie about who did what. `CoordinationStore::import_task` exists for exactly this
case: it creates the task directly in a caller-supplied `initial_state`, bypassing the transition
machine entirely, and records the `task_created` audit event's `to_state` as that state with
`details: {"imported": true}`, so the trail is honest about why a freshly created task can already
be non-`Pending`. `NewTask` itself gains no `initial_state` field for this — it is deserialized
directly from `mempalace_task_create`'s MCP arguments, and a new field there would silently widen
that public wire schema — so `import_task` is a separate entry point, not a hidden option on
`create_task`. A task imported directly into `Running` has `owner = NULL` and
`lease_expires_at = NULL` (no worker or lease is fabricated); it remains claimable by any worker
via `mempalace_task_claim`, since the "lease held by another worker" check only fires when an
owner already exists, and it cannot be swept into `Expired` by the absent lease — the only
automatic expiry check in `mempalace-storage` keys off `Task::expires_at` (the lifecycle deadline),
never `lease_expires_at`, and an import never sets `expires_at`. `import_task` rejects
`TaskState::Expired` as an initial state outright: expiry is a lifecycle outcome this palace
produces itself, never something an importer may assert about a task it has not yet placed under
this palace's lease/expiry rules.

The nine adapter tools are all `LocalOnly` — never federated, even when a wing routes coordination
writes to a remote:

- `mempalace_a2a_agent_card` — builds an A2A Agent Card (one skill per wing) from local wings and
  capabilities. `interfaces` (where an A2A client would reach this palace) has no local source and
  must be caller-supplied, possibly empty.
- `mempalace_a2a_task_import` — translates and persists an inbound A2A `Task` directly into its
  mapped `target_state` (with any coercion reported, e.g. `TASK_STATE_AUTH_REQUIRED` ->
  `input_required`) via `import_task`, per the state-preservation rule above. The response
  reports `replayed`: idempotency matches on `(created_by, idempotency_key)` alone, so a replay
  returns the task the *first* payload created rather than applying this one. A replay whose
  stored state disagrees with the state this payload maps to is refused as invalid params — the
  stored task is authoritative, and silently reporting the new payload's state, or filing a
  second protocol envelope contradicting the first, would misrepresent it. Use a distinct
  `idempotency_key` per task state.
- `mempalace_a2a_message_import` / `mempalace_a2a_artifact_import` — translate and persist an
  inbound A2A `Message`/`Artifact` via the ordinary `send_message`/`put_artifact` calls (messages
  and artifacts have no lifecycle state of their own to preserve).
- `mempalace_a2a_task_export` — translates an authoritative task back into an A2A `Task`.
  Artifacts and messages are not bulk-fetched (no such storage query exists); the caller passes
  the exact `artifact_ids`/`message_ids` to include, and any artifact with role `protocol_envelope`
  is always excluded from the emitted A2A artifacts list — it is an audit record, not an A2A
  artifact.
- `mempalace_mcp_tasks_get` — translates an authoritative task into an MCP Tasks `DetailedTask`
  (the `tasks/get` result shape). A missing `task_id` is rejected as invalid params (JSON-RPC
  `-32602`), matching the extension's own mandate for an invalid `taskId`.
- `mempalace_mcp_tasks_update` / `mempalace_mcp_tasks_cancel` — transition a task using an inbound
  MCP Tasks status, under the same compare-and-swap revision semantics as
  `mempalace_task_transition`: a revision conflict is returned as `{"success": false, "conflict":
  {...}}` data, never a JSON-RPC error. Three behaviours are specific to this surface, because
  MCP Tasks has neither a queued state nor a claim/lease concept:
  - **Pending -> Running bridge.** `allowed_transition` has no `Pending -> Running` edge — the
    only route into `Running` is a claim. Since `mempalace_mcp_tasks_get` shows a `Pending` task
    as `working`, an MCP-only client would otherwise be unable to advance the task it was just
    shown. `mempalace_mcp_tasks_update` therefore claims it first, for `actor`, with
    `lease_seconds`, and reports `bridged_from_pending`.
  - **Same-status updates are a no-op.** Re-sending the status a task already holds is an
    ordinary progress ping, but there is no self-transition edge, so it would otherwise fail.
    It returns `no_op: true` instead. `expected_revision` is still checked, so a stale repeat
    conflicts; and ownership is still enforced, so an actor who never claimed the task is
    refused exactly as it would be for any other target state. (Cancellation is never
    owner-gated, so `mempalace_mcp_tasks_cancel`'s equivalent no-op has no such check.)
  - **`details` on the bridge.** A `task_claimed` audit event carries no `details` field, so a
    `details` payload cannot be recorded when the call resolves as a claim. The response reports
    `details_recorded` rather than discarding it silently.
- `mempalace_mcp_tasks_import` — translates and persists an inbound `CreateTaskResult` directly
  into its mapped `target_state` via `import_task`, per the state-preservation rule above. It
  reports `replayed` and refuses a state-mismatched replay on the same terms as
  `mempalace_a2a_task_import`. `ttlMs`
  is a retention hint, never a MemPalace lifecycle deadline — it is surfaced only as
  `provenance.retention_deadline`, never written to the task's `expires_at`. `NewTask` has no
  column for the source `taskId`/`createdAt`/`lastUpdatedAt` either, so this tool returns them all
  as `provenance`; **the caller must persist `provenance` itself** (e.g. as a knowledge-graph fact)
  if it needs to resolve the wire task id or round-trip the original timestamps after a restart —
  this tool has no side channel to do that on the caller's behalf.

Why `LocalOnly`: translate-and-persist is a two-write sequence (the task or message/artifact
write, then the envelope-artifact write for task imports) with no remote transaction to make it
atomic. Federating either write independently would risk a task committed on one side with no
matching envelope, or vice versa, so these tools always run entirely against the local palace
regardless of the wing's `federation.coordination` rule.

## Recovery and maintenance

Coordination tables participate in the same SQLite WAL and backup procedures as other operational state. Back up `storage.sqlite3` consistently with the palace. Restore it before restarting workers; workers should then reread exact references and resume polling from their last committed cursor. No semantic index rebuild is needed for coordination state.
