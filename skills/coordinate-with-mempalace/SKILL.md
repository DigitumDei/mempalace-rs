---
name: coordinate-with-mempalace
description: Coordinate multiple agents or resumable work with existing MemPalace drawer and change-cursor tools. Use for manager-as-tools delegation, explicit agent handoffs, restart-safe work queues, or passing large results by stable reference without copying transcripts. This experimental skill does not provide atomic claims or reliable queue delivery.
---

# Coordinate with MemPalace

Use drawers as a durable coordination log and opaque change cursors as discovery checkpoints. Do not use semantic search as a queue.

## Start

1. Read `references/envelopes.md` before emitting an envelope.
2. Choose one workflow:
   - Read `references/manager-as-tools.md` when one manager invokes workers and retains control.
   - Read `references/explicit-handoff.md` when responsibility moves between agents or sessions.
3. Create a unique `coordination_id` and `task_id`. Keep IDs unchanged through the workflow.
4. Capture each remote origin's opaque cursor from `mempalace_get_changes_since`; resume that remote with its returned `next_cursor`, never a synthesized timestamp. The Phase 0 API has no local cursor: for local discovery, poll a bounded time window with intentional overlap, de-duplicate by drawer ID and `idempotency_key`, and report the audit as inconclusive if the per-origin limit is reached.

## Coordinate

1. File a `task` envelope verbatim with `mempalace_add_drawer` in an explicitly chosen wing and room.
2. Treat the returned drawer ID and origin as the stable reference. For a local write, construct `origin` as the literal `"local"`; for a remote write, preserve the `remote:<name>` origin reported by the routed operation. Pass the complete `{drawer_id, wing, room, origin}` reference, not a copied transcript.
3. For explicit transfer, file a `handoff` envelope referencing the task drawer, then have the recipient acknowledge it with a new envelope before work begins.
4. Store every substantial output once as an `artifact` envelope. Put the durable content in `payload.content` and record the returned drawer ID.
5. File a `result` envelope that references the task and artifact drawer IDs. Keep its summary small.
6. Poll remote changes by cursor and local changes with a bounded, overlapping timestamp window to discover candidate events. If a local response reaches the requested limit, widen or subdivide the audit window; because events sharing a timestamp cannot be paginated authoritatively, report the audit as inconclusive if the limit remains saturated. If the installed API has no exact get-by-ID operation, search may locate content for this experiment, but delivery remains unverified; never describe that fallback as authoritative dereferencing.
7. Record measurements from `references/measurements.md`.

Validate local envelope files with:

```bash
python3 scripts/validate_envelope.py path/to/envelope.json
```

## Safety rules

- Never claim exactly-once delivery, atomic task claiming, ordering across origins, or compare-and-set updates.
- Never infer ownership merely from semantic-search ranking.
- Require an idempotency key and ignore a repeated logical action already recorded by the consumer.
- Keep secrets out of envelopes unless the selected palace and federation policy explicitly permit them.
- Stop or escalate on conflicting claims; existing tools cannot resolve them atomically.
- Treat missing events as uncertain delivery and reconcile by known IDs when the API permits it, or report an explicit bounded audit as inconclusive.

## Finish

File the result and measurement summary, retain the final cursors, and report stable drawer references plus known delivery uncertainty. This skill is an experiment; recommend native coordination primitives for workflows requiring atomic claims, acknowledgements, ordered mailboxes, leases, or reliable delivery.
