# Skill registry

MemPalace stores reusable, governed procedures in the palace's local `storage.sqlite3`. A skill
is a versioned, provenance-carrying procedure that an agent can discover, apply, and record
outcomes against. MemPalace persists and governs the records; the host agent runtime still
chooses when to apply a skill, executes it, and selects evaluators and approval policy. The
registry is local-only and is not federated.

This is the first half of the Phase 2 work described in
[Coordination Phase 2 Design](Coordination-Phase-2-Design.md); delegation-loop telemetry is not
implemented yet.

## Data and guarantees

- A skill version is identified by the stable pair `(skill_id, version)`. Exact retrieval by
  that pair is authoritative and returns `found: false` on a miss; it never falls back to
  semantic search.
- Versions are derived by the store, never supplied by the caller: proposing against an
  existing `skill_id` allocates `max(version) + 1` and records the version that was latest at
  proposal time in `supersedes_version`. That field is provenance only — what this version was
  written against. It is **not** what promotion supersedes; see below.
- Proposals and outcome records deduplicate only on `(actor, idempotency_key)`. Replaying a key
  returns the committed record. Similar content under different keys stays distinct.
- Every lifecycle transition appends a `skill_reviews` row in the same SQLite transaction,
  identifying the reviewer and the exact status change. Nothing is overwritten silently.
- Promotion and retirement take an expected revision. A stale revision returns an explicit
  conflict rather than a last-write-wins update.

Skill statuses are `candidate`, `promoted`, `superseded`, and `retired`. `superseded` and
`retired` are terminal and cannot transition again. Retirement preserves the version and its
audit trail rather than deleting it.

Skill scopes are `agent`, `project`, and `organization`.

`project` scope requires a **`wing`** naming the owning project (for example
`wing_myproject`); `agent` and `organization` scope must omit it, because neither is
project-bound. A skill stays bound to its wing for its whole life — a later version cannot move
it to another project, which would otherwise be a way to hand an established `skill_id` to a
different project without review.

The wing is normalised the same way on every path that touches it — proposing and discovery
agree on `myproject` and `wing_myproject` naming the same wing, so a skill proposed under either
spelling is found by a discovery filter using either spelling. `ensure_schema` backfills any wing
stored before this normalisation existed, so a skill proposed under a raw, unprefixed wing before
the fix shipped becomes discoverable the same way once the palace has opened under the new code.

A palace holds many projects side by side, so a project-scoped skill that carried no project
identity would be authoritative everywhere, which is `organization` scope wearing the wrong
name. Passing `wing` to discovery hides project-scoped skills owned by *other* wings while
keeping agent- and organization-scoped ones visible. Omitting `wing` spans every project and is
an administrative view, not what a project-scoped agent should use.

`skill_id` remains a palace-wide identifier, like a package name — two projects wanting their
own `deploy` skill should name them distinctly.

## Promotion governance

Promotion is where "a candidate must not silently become an authoritative shared procedure" is
enforced in storage, not merely by convention:

- `agent` scope: **only the author** may promote the skill. An individual agent governs its own
  procedures, and nobody else's.
- `project` and `organization` scope: promotion requires **both** a `reviewer` different from
  the skill's `author` **and** at least one recorded outcome against that exact version.
  Promotion is rejected otherwise.

Promoting a version atomically supersedes whichever version is authoritative **at that moment**,
in the same transaction, so a skill never has two authoritative versions at once. This targets
the currently-promoted version rather than the numerically preceding one: versions can be
promoted out of order, or skipped entirely, and the invariant still holds.

Governance is the **stricter** of the promoted version's own scope and the scope of the version
it displaces. Proposing an `agent`-scoped successor to a promoted project- or organization-scoped
skill therefore does not escape shared review — the displaced version's scope still governs. A
scope change is a change to a governed record, not a way around the governance.

Actor IDs are asserted by the local host runtime. MemPalace enforces the author/reviewer
distinction and scope rules against those IDs; transport-level authentication remains a host
responsibility, as it is for coordination.

Applicability text, instruction references, and outcome notes are limited to 1 MiB.
Idempotency keys are limited to 256 bytes. Confidence must be within `0.0..=1.0`.

## Instruction references

`instructions_ref` is an opaque reference to where the procedure actually lives — a repository
path such as `skills/coordinate-with-mempalace/SKILL.md`, or a drawer ID. The registry stores
the reference and its provenance rather than copying instruction bodies, so a skill record stays
small and the source of truth stays where it is authored.

## MCP tools

The local skill-registry tool surface is:

- `mempalace_skill_propose` (takes `wing` for project scope), `mempalace_skill_get`,
  `mempalace_skill_versions`, `mempalace_skill_list` (takes `wing` to scope discovery)
- `mempalace_skill_record_outcome`
- `mempalace_skill_promote`, `mempalace_skill_retire`
- `mempalace_skill_reviews`

`mempalace_skill_list` is discovery only, and its `limit` is clamped to `1..=500`. Resolve a
specific version with `mempalace_skill_get` before treating a skill as authoritative, for the
same reason semantic search is not authoritative delivery in
[Native Coordination](Coordination.md).

`mempalace_skill_promote` and `mempalace_skill_retire` return
`{"success": false, "conflict": {...}}` on a revision mismatch, matching the conflict shape used
by the lineage and self-observation tools.

## Recovery and maintenance

Skill tables participate in the same SQLite WAL and backup procedures as other operational
state. Back up `storage.sqlite3` consistently with the palace. No semantic index rebuild is
needed for skill records.
