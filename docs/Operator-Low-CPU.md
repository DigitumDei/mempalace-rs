# Low-CPU Operator Guide

This guide covers the Rust `low_cpu` profile as an operating mode, not as a blanket hardware guarantee.

## Intent

Use `low_cpu` when the host is constrained enough that the balanced profile is the wrong default tradeoff.

The Rust runtime pins a separate profile name and applies stricter execution clamps through config resolution.

## Enable The Profile

In `~/.mempalace/config.json`:

```json
{
  "version": 1,
  "embedding_profile": "low_cpu"
}
```

Or per-process:

```bash
MEMPALACE_EMBEDDING_PROFILE=low_cpu target/release/mempalace-cli status
```

## Default Low-CPU Runtime

Default resolved values:

- `worker_threads = 1`
- `max_blocking_threads = 1`
- `queue_limit = 32`
- `ingest_batch_size = 8`
- `search_results_limit = 5`
- `wake_up_drawers_limit = 8`
- `degraded_mode = true`
- `rerank_enabled = false`

Default effective degraded limits:

- queue limit capped at `8`
- ingest batch size capped at `4`
- search results capped at `3`
- wake-up drawers capped at `4`

## Recommended Operating Pattern

1. Keep `degraded_mode = true` on the smallest hosts unless measurements on the target machine justify relaxing it.
2. Leave `rerank_enabled = false` unless the target host has proven headroom.
3. Prefer smaller, more frequent ingest runs over large one-shot bulk imports.
4. Validate `search` and `wake-up` output quality on the actual host after enabling the profile.

## Warm Cache Expectations

Low-CPU mode still depends on local model assets.

Operational rule:

- A constrained host with no warm cache is still a cold host.

Before declaring the node ready:

1. Resolve startup validation to `ready`.
2. Run a small ingest.
3. Run repeated search requests to observe the warmed path.

## Tuning

Supported knobs in `low_cpu`:

- `worker_threads`
- `max_blocking_threads`
- `queue_limit`
- `ingest_batch_size`
- `search_results_limit`
- `wake_up_drawers_limit`
- `degraded_mode`
- `rerank_enabled`

Safe tuning advice:

- Increase one knob at a time.
- Keep `worker_threads` and `max_blocking_threads` low on very small VMs.
- Raise `search_results_limit` only after verifying latency is still acceptable.
- Disable `degraded_mode` only after measuring the full query and wake-up path on the target host.

## Failure Modes

### Throughput is too low during ingest

Actions:

- Confirm `degraded_mode` is expected.
- Inspect `ingest_batch_size`.
- Break imports into smaller waves and avoid competing host load.

### Search looks too shallow

Actions:

- Check whether `search_results_limit` has been clamped by low-CPU mode.
- If the host can tolerate it, raise the configured limit or disable degraded mode.

### Wake-up context is too thin

Actions:

- Check `wake_up_drawers_limit`.
- Re-measure on the real host before relaxing clamps.

## What This Guide Does Not Claim

- It does not certify that every generic VM can pass the final low-CPU acceptance gate.
- It does not replace the benchmark and low-CPU validation suite from Phase 12.
- It does not imply parity with the balanced profile on recall or latency without host-specific measurement.
