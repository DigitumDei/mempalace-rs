# Driving external coding agents through MemPalace tasks

MemPalace coordination is a durable control plane, not a process scheduler. A task can describe,
lease, audit, and preserve delegated work across restarts, but creating or assigning a task does
not start OpenCode (or any other coding harness). A supervisor or human must still launch the
worker. One practical bridge is to invoke OpenCode's CLI in the intended worktree and tell it to
claim one exact MemPalace task:

```powershell
opencode run --agent build --format json `
  "Use the MemPalace task system. Read task <task-id> and assignment message <message-id>. `
  Claim it as agent opencode, implement only its bounded ownership, run its checks, publish a `
  result, and transition it to input_required for senior review. Do not commit or push."
```

The task description remains authoritative. The launch prompt should contain identifiers and
workflow policy, not a second copy of the implementation specification that can drift from the
task. Run the command with the repository worktree as its current directory.

## Recommended supervised workflow

1. Create a task with explicit repository, worktree, branch, file ownership, acceptance criteria,
   verification commands, budget, and prohibited actions.
2. Send one assignment message containing the task ID, worktree, and any delegation span ID.
3. Start the external agent explicitly. Require it to retrieve and claim the exact task before
   editing, and to preserve unrelated dirty-worktree changes.
4. Require periodic task-scoped inbox checks while it works. Use targeted `message_get` when a
   message ID is known; a full historical inbox can be large and noisy.
5. Have the worker publish a result and transition to `input_required`, not directly to
   `completed`, when a senior agent still owes a code review.
6. Review the actual diff and checks. Send findings as task messages. Start another bounded CLI
   turn for corrections if the worker process has already exited.
7. Only after review passes, transition the task to `completed` and close its delegation span.
   Task state and span state are separate records; neither closes the other automatically.

Tasks that can edit overlapping files should not run concurrently. Independent ownership can run
in parallel, but the supervisor still owns integration and full-workspace verification.

## Observed rough edges

- **Assignment does not wake a worker.** A pending task and addressed message remain passive until
  a human, scheduler, or supervising agent launches the target harness.
- **Terminal state is durable; terminal sessions are not.** A Codex/OpenCode restart loses the
  local process handle and streamed output, while the MemPalace task, messages, result, and diary
  survive. Resume by rereading task state, not by assuming the old terminal still represents it.
- **The desktop UI may hide a live child process.** A CLI process launched from a tool call may
  continue running even when no background-process indicator is visible. In one observed run, the
  process became apparent only after the user pressed **Stop**. Supervisors should report the
  process/session ID explicitly and verify liveness from the process handle and durable task state;
  the UI indicator must not be treated as the authority.
- **OpenCode needs its user-level runtime directory.** A launch from the Codex sandbox failed
  while opening `~/.local/share/opencode/log/opencode.log`; no OpenCode process or Windows handle
  owned the file, so this was a sandbox write restriction rather than a stale lock. Run the
  long-lived server from a normal user PowerShell (or grant the runtime directory explicitly),
  and distinguish launch failure from task failure. Do not force-close a supposedly locked log
  handle when Resource Monitor finds no owner.
- **A fresh XDG profile has no provider credentials.** Pointing an attached client at a temporary
  `XDG_DATA_HOME` is useful for isolating client state, but it does not carry over OpenRouter
  connections, model selection, or other user auth. Keep credentials in the authenticated
  `opencode serve` profile and use `opencode run --attach http://127.0.0.1:<port>` for workers.
- **Headless external-worktree permissions need an explicit policy.** An attached `opencode run`
  without `--auto` hit a worktree `external_directory` prompt and exited after the prompt was
  rejected. For a deliberately bounded task, launch with `--auto` only after the task has claimed
  the exact worktree and ownership; otherwise use an interactive approval path.
- **Late feedback is pull-based.** Sending a review message does not interrupt or wake an active
  OpenCode CLI turn. The worker must poll at sensible boundaries. A fast worker can publish a
  result and complete before seeing a late finding; `input_required` creates a safer review gate.
- **A complete task is not proof of a clean process exit, and vice versa.** After CLI termination,
  retrieve the task and result authoritatively. After a restart, do the same before relaunching so
  completed work is not duplicated.
- **A long CLI session can exit successfully without completing its protocol.** One resumed
  OpenCode session accumulated a very large context, exited with code 0 during analysis, made none
  of the requested corrective edits, and left its task `running` without a result. Recovery should
  start a fresh bounded session from the task and exact review-message IDs; process exit code alone
  is not evidence of task success.
- **Full inbox reads do not scale well for launch context.** An agent with a long-lived identity
  can receive a very large historical inbox; CLI JSON output may be truncated and distract from
  the current assignment. Prefer exact message retrieval plus a task-scoped or recent inbox read.
- **Streaming JSON is useful but too verbose as the system of record.** Tool traces can overwhelm
  terminal output limits. Treat them as progress telemetry; the concise MemPalace result and task
  transition are the durable completion report.
- **Leases need an execution policy.** A long run must renew its task lease before expiry. The
  task store records the lease, but the CLI bridge currently relies on prompting the worker to
  renew; it does not provide a supervisor heartbeat automatically.
- **File ownership is social unless the launcher enforces it.** Task text can bound ownership, but
  the filesystem does not prevent a worker from touching concurrent changes. Review `git diff`
  and serialize work that shares a crate or generated file.
- **Repository policy must be repeated at the execution boundary.** A task should explicitly say
  whether commits, pushes, branch switches, dependency downloads, or PR creation are allowed.
  The CLI bridge must not infer those permissions from the ability to edit the worktree.
- **Delegation telemetry has a separate lifecycle.** If a worker completes a task but cannot close
  the supervisor-owned span, the supervisor must reconcile and close it after verifying the
  result.

## Improvements worth automating

A small launcher can remove most of the manual ceremony. It should accept a task ID, retrieve the
task and assignment, verify the expected worktree and a non-overlapping ownership lock, start the
configured harness, renew the lease, forward new task messages to the live process, bound and
archive streamed output, then verify that a result exists before allowing a terminal transition.
It should recover after restart by scanning owned `running`/`input_required` tasks rather than by
persisting terminal session IDs as authority.

For OpenCode specifically, process exit is only transport completion. The launcher should parse
NDJSON and reject a run containing a top-level `error`, a tool part whose state is `error`, a
missing or ambiguous terminal event, or no final result. It should persist and display the PID,
OpenCode session ID, run ID, worktree, log path, and start time immediately, and poll both the OS
process and OpenCode session state. Fresh sessions are preferable after a bounded turn/time/token
threshold; never use implicit `--continue` when concurrent sessions or worktrees exist.

The more robust bridge is a persistent `opencode serve` process per worker/project. Health-check
it, subscribe to its event stream before prompting, use session status/messages as the OpenCode
completion signal, and use its abort endpoint for cancellation. MemPalace result/task state remains
the workflow completion signal. On Windows, preflight the OpenCode data/log directory, redirect
long-lived child stdout/stderr, retain the wrapper PID, and avoid relying on inherited terminal
pipes or the desktop background-process indicator.

The task API would also benefit from a task-filtered inbox/cursor and an optional scheduler hook
that emits a wake event when a pending task is assigned. Those features should remain separable:
durable coordination must continue to work even when no harness launcher is installed.

## Relevant upstream OpenCode reports

- Exit code 0 despite a rejected/error tool: <https://github.com/anomalyco/opencode/issues/36413>
- Silent exit 0 with an unknown finish reason: <https://github.com/anomalyco/opencode/issues/43622>
- `--continue` can select/inject into another active session: <https://github.com/anomalyco/opencode/issues/43133>
- Long headless sessions exhibit severe per-step slowdown: <https://github.com/anomalyco/opencode/issues/30067>
- Very large resumed sessions can fail at step zero: <https://github.com/anomalyco/opencode/issues/43459>
- Headless permission prompts can hang: <https://github.com/anomalyco/opencode/issues/36762>
- `opencode run` output may remain buffered until completion: <https://github.com/anomalyco/opencode/issues/22243>
- Windows child processes can retain output handles and prevent completion:
  <https://github.com/anomalyco/opencode/issues/32504>
- Official CLI and server contracts: <https://dev.opencode.ai/docs/cli/> and
  <https://dev.opencode.ai/docs/server/>
