# Measurement record

For every run record:

- attempted and completed tasks; completion rate is completed / attempted;
- duplicate executions, identified by repeated task ID with distinct result producers;
- lost updates, identified by an expected message ID absent after bounded cursor polling and exact-ID reconciliation;
- input/output token counts when the harness exposes them; otherwise record `not_available`;
- monotonic start/end times and elapsed milliseconds;
- restart recovery: whether a fresh process completed using IDs and cursors without a transcript;
- payload bytes and reference bytes, so the avoided transcript/output copying is explicit;
- every local and remote `next_cursor`, preserved as an opaque string.

Measurements are observational. They do not establish exactly-once or lossless delivery.
