# Envelope protocol

All envelopes are UTF-8 JSON objects. Version `mempalace.coordination/v1alpha1` is experimental and additive-only: consumers must ignore unknown fields but reject unknown `kind` or `api_version` values.

## Common fields

| Field | Requirement |
|---|---|
| `api_version` | Exactly `mempalace.coordination/v1alpha1` |
| `kind` | `task`, `handoff`, `result`, or `artifact` |
| `coordination_id` | Stable ID for one workflow |
| `message_id` | Unique ID for this envelope |
| `created_at` | RFC 3339 timestamp |
| `producer` | Stable agent/session name |
| `idempotency_key` | Stable key for one logical action |
| `payload` | Kind-specific object |

Use opaque, collision-resistant IDs such as UUIDs. References have `drawer_id`, `wing`, `room`, and `origin`; consumers must use the exact drawer ID and origin as authority. Set `origin` to `local` for a local write; preserve `remote:<name>` for a routed remote write. The Phase 0 API cannot dereference this pair directly, so reference passing is durable but reference retrieval is not yet reliable.

## Kinds

### `task`

`payload` requires `task_id`, `objective`, `acceptance_criteria` (array), and `constraints` (array). Optional `input_refs` contains references.

### `handoff`

`payload` requires `task_id`, `from`, `to`, `reason`, and `task_ref`. Optional `context_refs` contains references. A handoff transfers intended responsibility but is not an atomic claim. The recipient acknowledges by filing another `handoff` whose `reason` starts with `ack:` and references the same task.

### `artifact`

`payload` requires `artifact_id`, `media_type`, `content`, and `sha256`. Hash the exact UTF-8 bytes in `content`. File substantial output once and retain the returned drawer ID.

### `result`

`payload` requires `task_id`, `status` (`completed`, `blocked`, or `failed`), `summary`, `task_ref`, and `artifact_refs` (array). Keep `summary` concise; artifact references carry the output. Scope the idempotency key to one logical result by including a stable assignment, producer, or result-role component; retries of that result reuse the same key, while worker and manager results use different keys.

Templates are in `assets/*.json`. Run `scripts/validate_envelope.py` before filing when working from a local file.
