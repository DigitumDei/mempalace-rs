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

Delivery is at least once with idempotent writes. MemPalace does not promise exactly-once task execution.

Task states are `pending`, `running`, `input_required`, `completed`, `cancelled`, `failed`, and `expired`. Terminal states cannot transition again. Only a current owner may transition owned work, except that another actor may request cancellation. Only the addressed recipient may acknowledge a message.

Actor IDs are asserted by the local host runtime. MemPalace enforces ownership and recipient checks against those IDs; transport-level authentication and worker execution remain host-runtime responsibilities.

Payloads and artifact content are limited to 1 MiB. Idempotency keys are limited to 256 bytes.

## MCP tools

The native local tool surface is:

- `mempalace_task_create`, `mempalace_task_get`, `mempalace_task_claim`, `mempalace_task_renew`, `mempalace_task_transition`
- `mempalace_message_send`, `mempalace_message_get`, `mempalace_message_acknowledge`, `mempalace_inbox_read`
- `mempalace_artifact_put`, `mempalace_artifact_get`
- `mempalace_result_put`, `mempalace_result_get`
- `mempalace_coordination_event_get`, `mempalace_coordination_events`

Treat returned cursors as opaque and persist them with worker state. After restart, retrieve known task, message, result, and artifact IDs directly, then continue the inbox or event stream from the stored cursor.

## Recovery and maintenance

Coordination tables participate in the same SQLite WAL and backup procedures as other operational state. Back up `storage.sqlite3` consistently with the palace. Restore it before restarting workers; workers should then reread exact references and resume polling from their last committed cursor. No semantic index rebuild is needed for coordination state.
