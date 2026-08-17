# Self-Continuity Across Models

MemPalace can preserve a coherent agent self while the model or harness changes. It does this
without pretending that every engine behaves identically and without letting a single session
silently rewrite identity.

The persistent self is assembled from five distinct layers:

1. **Constitution** — `identity.txt`: durable identity, values, boundaries, and the working
   relationship. It is loaded on every wake-up.
2. **Lineage** — a stable, provider-neutral identifier for the continuing collaborator. A lineage
   is not a model name or a harness session.
3. **Reviewed self-observations** — evidence-backed working patterns that have been explicitly
   promoted. Unreviewed candidates are kept separate.
4. **Migration records** — explicit accounts of what carried over and what changed when the model
   or harness changed.
5. **Experience** — drawers, diary entries, knowledge-graph facts, and change history. These remain
   the evidence base rather than being copied wholesale into the constitution.

This separation lets memory and personality span engines while keeping engine-specific behavior
honest and revisable.

## Wake-up behavior

`mempalace_wake_up` now includes an `identity_packet` in addition to the existing identity,
status, change, project, and diary sections. Pass `agent_name` and, when known, the current `model`
and `harness`.

The packet contains:

- `packet_version`
- the constitution and its path
- the MCP-bound lineage, or the palace default when the server has no binding
- `lineage_selection`, including the selected ID, its source, and
  `override_allowed: false`; when a bound ID is missing, it also reports the requested ID and
  creation guidance
- promoted observations that apply to the current runtime
- recent migration records
- runtime metadata and compilation time

Inside the wake-up response, the packet's constitution uses `identity_ref: "$.identity"` to point
to the existing top-level identity instead of repeating the complete text. A standalone
`mempalace_identity_packet` response includes the constitution text directly.

Candidates are excluded by default. Set `include_candidates: true` only when reviewing or
developing the self-model. If no default lineage exists, wake-up still succeeds and returns
`configured: false` with guidance and any available lineages.

`lineage_id` is deliberately absent from both model-facing packet tools. If a client sends it
anyway, the call is rejected. The `agent_name`, `model`, and `harness` fields describe the current
runtime for attribution and engine-scoped filtering; none of them can select a lineage.

Engine-scoped observations are included only when every model or harness constraint recorded on
the observation matches the runtime supplied to wake-up. Omitting runtime metadata therefore
does not accidentally load engine-specific behavior as universal identity.

`mempalace_identity_packet` compiles the same structure on demand without performing the rest of
wake-up.

## Binding a lineage to an MCP client

Set `MEMPALACE_LINEAGE_ID` in the environment of the local `mempalace-mcp` process to bind that
MCP connection to one lineage. This is a host-controlled capability boundary: the model cannot
override the binding in a tool call, even if it knows another valid lineage ID. A configured ID is
syntax-checked when the server starts. If the ID does not exist in the palace, identity packet and
wake-up calls use the palace default for that response and include a `lineage_selection.message`
explaining how to create the requested lineage with `mempalace_lineage_set`.

Different MCP clients can share the same palace while binding their separately launched server
processes to different lineages. For example, Codex can use `codex-dion` while OpenCode uses
`opencode-dion`.

Codex `config.toml`:

```toml
[mcp_servers.mempalace]
command = "/absolute/path/to/mempalace-mcp"

[mcp_servers.mempalace.env]
MEMPALACE_LINEAGE_ID = "codex-dion"
```

Equivalent Codex CLI registration:

```bash
codex mcp add mempalace --env MEMPALACE_LINEAGE_ID=codex-dion -- /absolute/path/to/mempalace-mcp
```

OpenCode V2 configuration:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "servers": {
      "mempalace": {
        "type": "local",
        "command": ["/absolute/path/to/mempalace-mcp"],
        "environment": {
          "MEMPALACE_LINEAGE_ID": "opencode-dion"
        }
      }
    }
  }
}
```

Changing the binding requires editing the MCP host configuration and restarting that server
connection. When `MEMPALACE_LINEAGE_ID` is absent, packet tools use only the palace's stored
default lineage; there is still no per-call override. When a bound lineage is newly created with
`mempalace_lineage_set`, retry wake-up or identity-packet compilation and the binding will take
effect without changing the host configuration.

## Establishing a lineage

Use `mempalace_lineage_set` with a stable identifier that does not name the current provider or
model. The first lineage becomes the default automatically; `set_default: true` can move the
default later.

Lineage writes are revision-checked:

- use `expected_revision: 0` to create a lineage;
- use the current revision to update one;
- a stale revision returns `success: false` with the actual revision and makes no change.

This prevents concurrent sessions from silently overwriting each other's understanding of the
continuing self.

## Developing the self-model

Use `mempalace_self_observation_propose` when a repeated pattern may describe the persistent self.
Each proposal requires:

- a concise, falsifiable statement;
- a behavioral consequence explaining what should change if it is accepted;
- confidence from 0 to 1;
- one or more concrete evidence references;
- an author and lineage;
- optional counterevidence.

The scope controls portability:

| Scope | Meaning |
|---|---|
| `lineage` | Applies to the named lineage across models and harnesses. |
| `shared` | A deliberately shared working pattern available to every lineage in this local palace. The owning lineage remains recorded for provenance. |
| `engine` | Applies only when the packet's runtime matches the observation's recorded model and/or harness. At least one is required. |

New observations start as `candidate`. A candidate does not influence the default identity packet.
Use `mempalace_self_observation_review` with the current revision to `promote` or `retire` it and
record the reviewer and reason.

When a better observation replaces an older promoted one, propose it with
`supersedes_observation_id`. Promoting the replacement atomically marks the old observation
`superseded`, preserving the history without loading both accounts as current.

The lifecycle is:

```text
candidate -> promoted -> superseded
    |
    +------> retired
```

## Recording a model or harness change

Use `mempalace_migration_record` after comparing behavior across a model or harness transition.
A migration records:

- the old and new model/harness identifiers;
- a concise summary;
- continuities that still belong to the lineage;
- changes attributable to the new runtime;
- concrete evidence.

Migration records are context, not automatic identity edits. If a migration reveals a durable
pattern, propose a self-observation and review it separately. If it reveals a constitutional
change, update `identity.txt` deliberately.

## Constitution discipline

Treat `identity.txt` as a constitution, not an autobiography or an append-only session log.
`mempalace_identity_update` accepts at most 16 KiB of content per call, while the final file may be
up to 64 KiB. Use diaries for session experience and self-observations for developing patterns so
the constitution stays compact and legible on every wake-up.

## Locality and change history

The lineage, observation, review, and migration tools are local-only and are not federated. Their
mutations appear in `mempalace_get_changes_since` as `lineage_set`,
`self_observation_proposed`, `self_observation_reviewed`, and
`lineage_migration_recorded` events.

The implementation uses only local SQLite state and the existing local identity file. It adds no
external inference, telemetry, or network dependency.
