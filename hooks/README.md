# MemPalace Hooks — Auto-Save and Auto-Orient for Terminal AI Tools

These hook scripts make MemPalace work automatically. No manual "save" or "orient" commands needed.

## What They Do

| Hook | When It Fires | What Happens |
|------|--------------|-------------|
| **Init Hook** | First message of each session | Injects a reminder for the AI to read the palace before responding |
| **Save Hook** | Every 15 human messages | Blocks the AI, tells it to save key topics/decisions/quotes to the palace |
| **PreCompact Hook** | Right before context compaction | Emergency save — forces the AI to save EVERYTHING before losing context |

The AI does the actual filing — it knows the conversation context, so it classifies memories into the right wings/halls/closets. The hooks just tell it WHEN to act.

## Init Hook — Session Orientation

The init hook fires on the **first user message of each session** and injects context instructing the AI to call `mempalace_status` and `mempalace_diary_read` before it responds. This means Claude arrives in every session already oriented — it knows what project it's in, what was decided last time, and what still needs doing.

It uses a `/tmp/claude-mp-<session_id>` marker file to fire exactly once per session. The marker files are small and are cleared automatically on reboot.

The hook output uses the `UserPromptSubmit` hook's `additionalContext` field — a Claude Code mechanism that injects text into the AI's context without modifying the user's message.

## Install — Claude Code

The hooks are split across two settings files because they serve different scopes.

### Global settings — Init Hook

The init hook goes in **`~/.claude/settings.json`** (user-level, applies to all projects) because session orientation is always useful, not just in this repo.

**macOS / Linux:**
```json
{
  "hooks": {
    "UserPromptSubmit": [{
      "hooks": [{
        "type": "command",
        "command": "/Users/YOU/.claude/mempalace-init-hook.sh"
      }]
    }]
  }
}
```

**Windows (Git Bash / MSYS2 path format):**
```json
{
  "hooks": {
    "UserPromptSubmit": [{
      "hooks": [{
        "type": "command",
        "command": "bash /c/Users/YOU/.claude/mempalace-init-hook.sh"
      }]
    }]
  }
}
```

Copy the script to your Claude config directory first:
```bash
cp hooks/mempalace-init-hook.sh ~/.claude/mempalace-init-hook.sh
chmod +x ~/.claude/mempalace-init-hook.sh
```

### Project settings — Save and PreCompact Hooks

The save and precompact hooks go in **`.claude/settings.local.json`** in the project root (or your global `~/.claude/settings.json` if you want them everywhere):

```json
{
  "hooks": {
    "Stop": [{
      "matcher": "*",
      "hooks": [{
        "type": "command",
        "command": "/absolute/path/to/hooks/mempal_save_hook.sh",
        "timeout": 30
      }]
    }],
    "PreCompact": [{
      "hooks": [{
        "type": "command",
        "command": "/absolute/path/to/hooks/mempal_precompact_hook.sh",
        "timeout": 30
      }]
    }]
  }
}
```

Make them executable:
```bash
chmod +x hooks/mempal_save_hook.sh hooks/mempal_precompact_hook.sh
```

## Install — Codex CLI (OpenAI)

Add to `.codex/hooks.json`:

```json
{
  "Stop": [{
    "type": "command",
    "command": "/absolute/path/to/hooks/mempal_save_hook.sh",
    "timeout": 30
  }],
  "PreCompact": [{
    "type": "command",
    "command": "/absolute/path/to/hooks/mempal_precompact_hook.sh",
    "timeout": 30
  }]
}
```

## Configuration

Edit `mempal_save_hook.sh` to change:

- **`SAVE_INTERVAL=15`** — How many human messages between saves. Lower = more frequent saves, higher = less interruption.
- **`STATE_DIR`** — Where hook state is stored (defaults to `~/.mempalace/hook_state/`)
- **`MEMPAL_DIR`** — Optional. Set to a conversations directory to auto-run `mempalace mine <dir>` on each save trigger. Leave blank (default) to let the AI handle saving via the block reason message.

### mempalace CLI

The relevant commands are:

```bash
mempalace mine <dir>               # Mine all files in a directory
mempalace mine <dir> --mode convos # Mine conversation transcripts only
```

The hooks resolve the repo root automatically from their own path, so they work regardless of where you install the repo.

## How It Works (Technical)

### Init Hook (UserPromptSubmit event)

```
User sends first message of session → Claude Code fires UserPromptSubmit hook
                                                ↓
                                    Hook reads session_id from input JSON
                                                ↓
                              ┌─── /tmp/claude-mp-<id> exists ──→ echo "{}" (no-op)
                              │
                              └─── marker absent ──→ touch marker
                                                          ↓
                                                   Output additionalContext JSON
                                                          ↓
                                                   Claude Code injects context into AI turn
                                                          ↓
                                                   AI calls mempalace_status + diary_read
                                                          ↓
                                                   AI responds, already oriented
```

The `additionalContext` field is injected into the AI's context without modifying the user's message — the user never sees it. Subsequent messages in the same session hit the marker-exists branch and are no-ops.

### Save Hook (Stop event)

```
User sends message → AI responds → Claude Code fires Stop hook
                                            ↓
                                    Hook counts human messages in JSONL transcript
                                            ↓
                              ┌─── < 15 since last save ──→ echo "{}" (let AI stop)
                              │
                              └─── ≥ 15 since last save ──→ {"decision": "block", "reason": "save..."}
                                                                    ↓
                                                            AI saves to palace
                                                                    ↓
                                                            AI tries to stop again
                                                                    ↓
                                                            stop_hook_active = true
                                                                    ↓
                                                            Hook sees flag → echo "{}" (let it through)
```

The `stop_hook_active` flag prevents infinite loops: block once → AI saves → tries to stop → flag is true → we let it through.

### PreCompact Hook

```
Context window getting full → Claude Code fires PreCompact
                                        ↓
                                Hook ALWAYS blocks
                                        ↓
                                AI saves everything
                                        ↓
                                Compaction proceeds
```

No counting needed — compaction always warrants a save.

## Debugging

Check the save hook log:
```bash
cat ~/.mempalace/hook_state/hook.log
```

Example output:
```
[14:30:15] Session abc123: 12 exchanges, 12 since last save
[14:35:22] Session abc123: 15 exchanges, 15 since last save
[14:35:22] TRIGGERING SAVE at exchange 15
[14:40:01] Session abc123: 18 exchanges, 3 since last save
```

For the init hook, check whether the marker file exists for your session:
```bash
ls /tmp/claude-mp-*
```

If the marker exists but orientation didn't happen, the hook ran but the AI may have skipped the injected context. Try starting a fresh session (new marker = fresh trigger).

If orientation never fires at all, verify the script is executable and the path in your settings is correct.

## Cost

**Zero extra tokens.** The hooks are bash scripts that run locally. They don't call any API. The only "cost" is the AI spending a few seconds organizing memories at each checkpoint — and it's doing that with context it already has loaded.
