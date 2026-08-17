# Manager-as-tools workflow

Use when a manager remains responsible and invokes bounded workers.

1. Manager files one task envelope and retains its drawer ID.
2. Manager invokes each worker with only the task reference, its bounded assignment, and the current per-origin cursors.
3. Worker receives the stable task reference from the manager, produces one artifact envelope, then files a result referencing both task and artifact.
4. Worker returns only the result/artifact drawer IDs and updated cursors.
5. Manager polls `mempalace_get_changes_since` with every saved origin cursor, validates candidate envelopes, de-duplicates by `idempotency_key`, and merges referenced results. Without get-by-ID, retrieval is an experimental search fallback and must be reported as unverified.
6. Manager files the final result and measurements.

The manager must serialize assignment decisions. Concurrent workers can otherwise duplicate the same work because drawer creation does not atomically claim a task.
