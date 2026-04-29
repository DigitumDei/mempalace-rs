#!/usr/bin/env bash
# Fires on UserPromptSubmit. On the first message of each session, injects
# an additionalContext reminder to read MemPalace before responding.
INPUT=$(cat)
SESSION_ID=$(echo "$INPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('session_id','unknown'))" 2>/dev/null)
MARKER="/tmp/claude-mp-$SESSION_ID"
if [ ! -f "$MARKER" ]; then
  touch "$MARKER"
  echo '{"hookSpecificOutput":{"hookEventName":"UserPromptSubmit","additionalContext":"[Session orientation pending] Before responding to this message, call mempalace_status then mempalace_diary_read with agent_name=claude to orient yourself. Do this first."}}'
else
  echo "{}"
fi
