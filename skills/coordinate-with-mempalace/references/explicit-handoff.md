# Explicit-handoff workflow

Use when responsibility moves to another agent or a later process.

1. Sender files a task and records its drawer ID.
2. Sender files a handoff referencing the task, naming the intended recipient.
3. Recipient discovers the event via saved per-origin cursors, validates the supplied envelope, and checks its idempotency log. If only a reference crosses the process boundary, report that the current API cannot dereference it exactly.
4. Recipient files an acknowledgement handoff before work. The acknowledgement is evidence, not an atomic lease.
5. Recipient files output once as an artifact and files a result referencing task and artifact drawer IDs.
6. Sender or a restarted session resumes from saved cursors and attempts to recover the result. Search can demonstrate content recovery, but it cannot prove the returned content is the referenced drawer until get-by-ID exists.

If two recipients acknowledge the same task, stop and reconcile explicitly. Do not pick a winner using timestamps or semantic relevance.
