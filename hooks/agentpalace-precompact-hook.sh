#!/bin/bash
# AGENTPALACE PRE-COMPACT HOOK — Emergency save before compaction
#
# Claude Code "PreCompact" hook. Fires RIGHT BEFORE the conversation
# gets compressed to free up context window space.
#
# This is the safety net. When compaction happens, the AI loses detailed
# context about what was discussed. This hook forces one final save of
# EVERYTHING before that happens.
#
# Unlike the save hook (which triggers every N exchanges), this ALWAYS
# blocks — because compaction is always worth saving before.
#
# === INSTALL ===
# Add to .claude/settings.local.json:
#
#   "hooks": {
#     "PreCompact": [{
#       "hooks": [{
#         "type": "command",
#         "command": "/absolute/path/to/agentpalace-precompact-hook.sh",
#         "timeout": 30
#       }]
#     }]
#   }
#
# For Codex CLI, add to .codex/hooks.json:
#
#   "PreCompact": [{
#     "type": "command",
#     "command": "/absolute/path/to/agentpalace-precompact-hook.sh",
#     "timeout": 30
#   }]
#
# === HOW IT WORKS ===
#
# Claude Code sends JSON on stdin with:
#   session_id — unique session identifier
#
# We always return decision: "block" with a reason telling the AI
# to save everything. After the AI saves, compaction proceeds normally.
#
# === AGENTPALACE CLI ===
# This repo uses: agentpalace mine <dir>
# or:            agentpalace mine <dir> --mode convos
# Set AGENTPALACE_DIR below if you want the hook to auto-ingest before compaction.
# Leave blank to rely on the AI's own save instructions.

STATE_DIR="$HOME/.agentpalace/hook_state"
mkdir -p "$STATE_DIR"

# Optional: set to the directory you want auto-ingested before compaction.
# Example: AGENTPALACE_DIR="$HOME/conversations"
# Leave empty to skip auto-ingest (AI handles saving via the block reason).
AGENTPALACE_DIR=""

# Mine mode for the optional auto-ingest above: `convos` for chat transcripts
# (the usual AGENTPALACE_DIR contents), `projects` for source files.
AGENTPALACE_MINE_MODE="convos"

# Locate the agentpalace binary. Hooks run in a non-interactive shell that may
# not have the installer's PATH additions, so fall back to the default install
# location before giving up.
resolve_agentpalace_cli() {
    if command -v agentpalace >/dev/null 2>&1; then
        command -v agentpalace
        return 0
    fi
    for candidate in "$HOME/.agentpalace/bin/agentpalace" \
                     "$HOME/.agentpalace/bin/agentpalace.exe"; do
        if [ -x "$candidate" ]; then
            printf '%s\n' "$candidate"
            return 0
        fi
    done
    return 1
}

# Read JSON input from stdin
INPUT=$(cat)

SESSION_ID=$(echo "$INPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('session_id','unknown'))" 2>/dev/null)

echo "[$(date '+%H:%M:%S')] PRE-COMPACT triggered for session $SESSION_ID" >> "$STATE_DIR/hook.log"

# Optional: run agentpalace ingest synchronously so memories land before compaction
if [ -n "$AGENTPALACE_DIR" ] && [ -d "$AGENTPALACE_DIR" ]; then
    if AGENTPALACE_CLI="$(resolve_agentpalace_cli)"; then
        "$AGENTPALACE_CLI" mine "$AGENTPALACE_DIR" --mode "$AGENTPALACE_MINE_MODE" \
            >> "$STATE_DIR/hook.log" 2>&1
    else
        echo "[$(date '+%H:%M:%S')] agentpalace not found on PATH or in ~/.agentpalace/bin; skipping auto-ingest" \
            >> "$STATE_DIR/hook.log"
    fi
fi

# Always block — compaction = save everything
cat << 'HOOKJSON'
{
  "decision": "block",
  "reason": "COMPACTION IMMINENT. Save ALL topics, decisions, quotes, code, and important context from this session to your memory system. Be thorough — after compaction, detailed context will be lost. Organize into appropriate categories. Use verbatim quotes where possible. Save everything, then allow compaction to proceed."
}
HOOKJSON
