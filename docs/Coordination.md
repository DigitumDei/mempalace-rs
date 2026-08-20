# Native local coordination

MemPalace stores durable coordination state in the palace's local `storage.sqlite3`. It provides persistence and concurrency control; the host agent runtime still owns worker spawning, scheduling, tool execution, and live budget enforcement. Coordination is local-only and is not federated.

## Data and guarantees

- Tasks have immutable IDs, parent and dependency references, optional budget metadata and expiry, a lifecycle state, an owner, a lease expiry, and a monotonically increasing revision.
- Messages are append-only, addressed to one recipient, ordered by a local sequence, and explicitly acknowledged.
- Results are immutable structured payloads with stable IDs. Artifacts are immutable, carry a role and media type, and include a BLAKE3 content hash.
- Every mutation appends an audit event in the same SQLite transaction. Events identify the actor and task transition and are read with an opaque sequence cursor.
- Task creation, message send, result, and artifact writes deduplicate only on `(actor, idempotency_key)`. Replaying a key returns the committed record. Similar content with different keys remains distinct.
- Exact get operations query primary IDs only and return `found: false` on a miss. They never invoke semantic search.
- Claims, renewals, and transitions use an expected revision. A stale revision fails explicitly. A valid lease excludes other workers; an expired lease can be reclaimed without deleting prior events.
- Every task belongs to a wing, normalised the same way project wings are normalised everywhere else in the palace, so `myproject` and `wing_myproject` resolve to the same wing. Messages, artifacts, and results carry no wing column of their own and reach it through their mandatory task reference; audit events do carry their own `wing` column, always the owning task's wing materialised inside the same transaction as the mutation, never a value supplied by a caller.
- `wing_unscoped` is a reserved wing name meaning "created before wings existed." The operational schema upgrades itself in place the first time a palace opens under this code — existing tasks, messages, artifacts, results, and events are untouched and read back with that wing, with no separate migration step or data export/import.
- `mempalace_coordination_events` and `mempalace_inbox_read` accept an optional wing filter, normalised the same way as task creation, so a filter of `myproject` matches records stored under `wing_myproject`. Omitting the filter is unscoped and spans every wing, matching how visibility already worked before wings existed.

Delivery is at least once with idempotent writes. MemPalace does not promise exactly-once task execution.

Task states are `pending`, `running`, `input_required`, `completed`, `cancelled`, `failed`, and `expired`. Terminal states cannot transition again. Only a current owner may transition owned work, except that another actor may request cancellation. Only the addressed recipient may acknowledge a message.

Actor IDs are asserted by the local host runtime. MemPalace enforces ownership and recipient checks against those IDs; transport-level authentication and worker execution remain host-runtime responsibilities.

Task titles, descriptions, JSON payloads and budgets, and artifact content are limited to 1 MiB. Idempotency keys are limited to 256 bytes. Inbox and event cursors are `null` when a page contains the final available records; a non-null cursor indicates that another page is available.

## MCP tools

The native local tool surface is:

- `mempalace_task_create` (requires `wing`), `mempalace_task_get`, `mempalace_task_claim`, `mempalace_task_renew`, `mempalace_task_transition`
- `mempalace_message_send`, `mempalace_message_get`, `mempalace_message_acknowledge`, `mempalace_inbox_read` (takes an optional `wing` filter)
- `mempalace_artifact_put`, `mempalace_artifact_get`
- `mempalace_result_put`, `mempalace_result_get`
- `mempalace_coordination_event_get`, `mempalace_coordination_events` (takes an optional `wing` filter)

Treat returned cursors as opaque and persist them with worker state. After restart, retrieve known task, message, result, and artifact IDs directly, then continue the inbox or event stream from the stored cursor.

## Recovery and maintenance

Coordination tables participate in the same SQLite WAL and backup procedures as other operational state. Back up `storage.sqlite3` consistently with the palace. Restore it before restarting workers; workers should then reread exact references and resume polling from their last committed cursor. No semantic index rebuild is needed for coordination state.
