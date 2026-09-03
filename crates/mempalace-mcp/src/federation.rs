use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use mempalace_config::{
    FederationRuntimeConfig, ReplicationStatus, ResolvedRouteRule, RouteMode, RouteQuery,
    WriteTarget, resolve_coordination_route, resolve_kg_route, resolve_route,
};
use mempalace_core::{DIARY_ROOM, DIARY_TOPIC_PREFIX, SHARED_AGENT_DIARY_WING, WingId, hash_text};
use mempalace_federation::{
    AckMessageRequest, AddDrawerRequest, ChangesQuery, CoordinationEventsQuery,
    DrawerSearchRequest, InboxQuery, NewArtifactRequest, NewMessageRequest, NewTaskRequest,
    NewTaskResultRequest, RemoteDrawerResult, TaskLeaseRequest, TransitionTaskRequest,
};
use mempalace_remote::{
    RemoteApi, RemoteClient, RemoteEndpoint, RemoteError, RemoteRevisionedWrite,
};
use serde_json::{Value, json};
use tokio::task::JoinSet;

use crate::{McpError, ToolError, ToolResult};

pub struct FederationRouter {
    pub rules: FederationRuntimeConfig,
    pub remotes: BTreeMap<String, Arc<dyn RemoteApi>>,
}

impl fmt::Debug for FederationRouter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FederationRouter")
            .field("rules", &self.rules)
            .field("remotes", &self.remotes.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl FederationRouter {
    pub fn new(rules: FederationRuntimeConfig) -> Self {
        let mut remotes = BTreeMap::new();
        for (name, remote) in &rules.remotes {
            let endpoint = RemoteEndpoint {
                name: remote.name.clone(),
                base_url: remote.url.clone(),
                token: remote.token.clone(),
                timeout: remote.timeout,
            };
            match RemoteClient::new(endpoint) {
                Ok(client) => {
                    remotes.insert(name.clone(), Arc::new(client) as Arc<dyn RemoteApi>);
                }
                Err(error) => {
                    tracing::warn!(
                        remote = %name,
                        %error,
                        "failed to build remote client; excluding from federation"
                    );
                }
            }
        }
        Self { rules, remotes }
    }

    /// Direct-construction entry point for tests and callers that provide
    /// their own [`RemoteApi`] implementations.
    pub fn with_remotes(
        rules: FederationRuntimeConfig,
        remotes: BTreeMap<String, Arc<dyn RemoteApi>>,
    ) -> Self {
        Self { rules, remotes }
    }

    pub fn has_remotes(&self) -> bool {
        !self.remotes.is_empty()
    }

    /// True when coordination has actually been federated by configuration — as opposed to
    /// merely having *some* remote configured for other purposes (drawers, KG).
    ///
    /// The ID-referencing coordination fallbacks below (`coordination_read_fallback`,
    /// `coordination_write_fallback`, and the claim/renew/transition equivalents) have no wing
    /// to resolve a route against — that is the whole reason they exist (see the "Coordination
    /// (issue #102 Stage 4)" section comment above). Without this check they would fan out to
    /// every configured remote on every local miss regardless of whether the operator ever
    /// wrote a `federation.coordination` entry: an operator who federates drawers only, and
    /// never configures coordination, would still have a local `mempalace_task_get` miss sent
    /// to their remote. That violates "federation remains disabled unless explicitly
    /// configured per supported scope/wing" (issue #102's first acceptance criterion) and the
    /// local-first-by-default invariant this whole crate exists to protect.
    ///
    /// True when either an explicit `federation.coordination[wing]` entry exists (some wing was
    /// deliberately routed) or `default_mode` itself is non-`Local` (coordination falls through
    /// to it exactly like [`mempalace_config::resolve_coordination_route`] does, so the gate has
    /// to agree with that fallback, not just the explicit table). This does not depend on which
    /// wing a task actually belongs to — a coordination ID has no wing until a record naming it
    /// is found, local or remote — so, unlike a drawer or KG route, this is necessarily an
    /// all-or-nothing switch for federation being configured at all, not a per-wing decision.
    pub fn coordination_federation_enabled(&self) -> bool {
        !self.rules.coordination.is_empty() || self.rules.default_mode != RouteMode::Local
    }

    /// Compute wing availability annotations for the local wing set, keyed by
    /// wing name. `local_wings` is the set of wing names present in the local
    /// palace. Returns a map of `wing_name → "local" | "remote:<name>" | "combined"`.
    /// Uses the same resolution precedence as real routing (resolve_route with
    /// wing rule, then default_mode). Per-project routing is not wired at the
    /// MCP layer (always None) because the stdio server has no per-project context.
    pub fn wing_availability(&self, local_wings: &BTreeMap<String, usize>) -> Value {
        let mut avail = serde_json::Map::new();
        let all_wing_names: std::collections::BTreeSet<&str> = local_wings
            .keys()
            .map(|s| s.as_str())
            .chain(self.rules.wings.keys().map(|s| s.as_str()))
            .collect();

        for wing_name in all_wing_names {
            let rule = resolve_route(
                &self.rules,
                None,
                RouteQuery { wing: Some(wing_name), room: None, source_file: None },
            );
            let status = match rule.mode {
                RouteMode::Local => "local".to_owned(),
                RouteMode::Remote => {
                    if let Some(name) = &rule.remote {
                        format_remote_origin(name)
                    } else {
                        "remote".to_owned()
                    }
                }
                RouteMode::Combined => "combined".to_owned(),
            };
            avail.insert(wing_name.to_owned(), json!(status));
        }
        Value::Object(avail)
    }

    /// Compute coordination availability annotations for the local wing set, keyed by wing
    /// name. Sibling to [`Self::wing_availability`] but reports a **different thing**: the
    /// output shape here is `wing_name → "local" | "remote:<name>"`, the *effective write
    /// target* (where a `mempalace_task_create` for that wing would actually place the task),
    /// not the routing *mode*.
    ///
    /// This is deliberately not symmetric with `wing_availability`, which reports `"combined"`
    /// verbatim for `RouteMode::Combined`. For drawers that is a meaningful, complete answer —
    /// `write: both` is a real, supported drawer configuration (dual-write plus dual-read), so
    /// `"combined"` genuinely describes what happens. For coordination it is not: a task is
    /// authoritative in exactly one palace, `federation.coordination` rejects any rule that
    /// would resolve to `write: both` at config load (see
    /// [`mempalace_config::resolve_coordination_route`]'s doc comment), and
    /// `rule_from_default_mode` maps an inherited `RouteMode::Combined` to `write: local` — so a
    /// wing falling through `default_mode: combined` with no explicit coordination rule places
    /// every task locally while still carrying `mode == Combined`. Reporting the mode there
    /// would print `"combined"` for a wing that never puts a task anywhere but the local
    /// palace — answering the one question this field exists to answer (where will my task
    /// land?) incorrectly for every such wing. Reporting the resolved write target instead is
    /// correct precisely because coordination's `write` is always fully determined
    /// (`Local`/`Remote`, never `Both`) — there is no ambiguity here to lose by collapsing to a
    /// single value the way there would be for a hypothetical dual-write coordination wing.
    ///
    /// Uses [`Self::resolve_write_target`] rather than re-deriving the mode→target mapping here
    /// — that is the one implementation `add_drawer_remote`/`kg_add_remote`/`task_create` all
    /// already share, and re-deriving it in a diagnostic method would just recreate the seam
    /// that a future change to the precedence rule could drift out of sync on.
    ///
    /// This is intentionally *not* a re-derivation of `resolve_coordination_route`'s precedence
    /// either — it calls that function directly — so the diary hard-override (`wing_agents`
    /// always `"local"`), the wing normalisation, and the fail-closed behaviour on an
    /// unnormalisable wing all come from the single source of truth used by real coordination
    /// routing.
    ///
    /// The key set is `local_wings ∪ rules.wings ∪ rules.coordination`: `rules.wings` is
    /// included (not just `rules.coordination`) so this map has the same keys as
    /// `wing_availability` and the two can be read side by side — a wing configured only for
    /// drawers still gets a coordination answer (it falls through to `default_mode`), and
    /// surfacing that is the point. `rules.coordination` is included so a wing configured only
    /// for coordination (no drawers, no `federation.wings` entry) is visible at all — the
    /// concrete defect this method exists to fix.
    ///
    /// Per-wing coordination *reads* do not vary by mode the way writes do:
    /// `coordination_candidate_remotes()` is a union across every `federation.coordination`
    /// rule plus the default remote, tried in name order regardless of which wing a record
    /// belongs to (a coordination read has no wing to resolve against until the record is
    /// found — see the "Coordination (issue #102 Stage 4)" section comment above). Placement
    /// (this field) is therefore the only per-wing coordination semantic there is to report;
    /// reporting the write target loses no information a per-wing read behaviour would need.
    pub fn coordination_availability(&self, local_wings: &BTreeMap<String, usize>) -> Value {
        let mut avail = serde_json::Map::new();
        let all_wing_names: std::collections::BTreeSet<&str> = local_wings
            .keys()
            .map(|s| s.as_str())
            .chain(self.rules.wings.keys().map(|s| s.as_str()))
            .chain(self.rules.coordination.keys().map(|s| s.as_str()))
            .collect();

        for wing_name in all_wing_names {
            let rule = self.resolve_coordination_route(wing_name);
            let write_target = self.resolve_write_target(&rule);
            let status = match write_target {
                WriteTarget::Local => "local".to_owned(),
                WriteTarget::Remote => {
                    if let Some(name) = &rule.remote {
                        format_remote_origin(name)
                    } else {
                        "remote".to_owned()
                    }
                }
                // Structurally unreachable: `resolve_coordination_route` never returns
                // `write: both` — `resolve_federation_config` rejects any
                // `federation.coordination` entry that would resolve to it, at config load
                // (see `resolve_coordination_route`'s doc comment). Panicking here rather than
                // silently folding this into `"local"` or `"remote"` is deliberate: either
                // fallback would misreport where a task actually lands, and a value this
                // diagnostic exists to make trustworthy must not lie quietly if the load-time
                // invariant is ever broken.
                WriteTarget::Both => unreachable!(
                    "coordination route resolved to WriteTarget::Both for wing `{wing_name}`; \
                     resolve_federation_config should reject this at config load"
                ),
            };
            avail.insert(wing_name.to_owned(), json!(status));
        }
        Value::Object(avail)
    }

    /// Resolve the route for a drawer operation. Accepts room and source_file so
    /// the diary hard-override can fire correctly (see resolve_route precedence).
    pub fn resolve_drawer_route(
        &self,
        wing: Option<&str>,
        room: Option<&str>,
        source_file: Option<&str>,
    ) -> ResolvedRouteRule {
        resolve_route(&self.rules, None, RouteQuery { wing, room, source_file })
    }

    pub fn resolve_kg_route(&self) -> ResolvedRouteRule {
        resolve_kg_route(&self.rules)
    }

    /// Returns `true` when the resolved route indicates a dual-write
    /// (`write: both` — local write + best-effort remote replication).
    pub fn is_dual_write(&self, route: &ResolvedRouteRule) -> bool {
        route.mode == RouteMode::Combined && route.write == WriteTarget::Both
    }

    /// Resolves the effective write intent from a route rule.
    ///
    /// - `Local` mode or `Combined + write:local` → `WriteTarget::Local`
    /// - `Remote` mode or `Combined + write:remote` → `WriteTarget::Remote`
    /// - `Combined + write:both` → `WriteTarget::Both`
    pub fn resolve_write_target(&self, route: &ResolvedRouteRule) -> WriteTarget {
        match route.mode {
            RouteMode::Local => WriteTarget::Local,
            RouteMode::Remote => WriteTarget::Remote,
            RouteMode::Combined => route.write,
        }
    }

    fn remote_for_rule(&self, rule: &ResolvedRouteRule) -> Option<&Arc<dyn RemoteApi>> {
        rule.remote.as_ref().and_then(|name| self.remotes.get(name))
    }

    // ─── Search ──────────────────────────────────────────────────────────────────

    /// Plan the search fan-out for a query. Returns `(include_local, remote_targets)`.
    ///
    /// Diary guard fires first: if `wing == Some(SHARED_AGENT_DIARY_WING)` or
    /// `room == Some(DIARY_ROOM)` the result is always `(true, vec![])` — diary
    /// content is never federated.
    ///
    /// When `wing` is given, routing follows the resolved rule for that wing:
    /// - Local  → `(true, vec![])`
    /// - Remote → `(false, vec![remote])` (or `(false, vec![])` if the remote is
    ///   not built / has no name)
    /// - Combined → `(true, vec![remote])` (or `(true, vec![])` as above)
    ///
    /// When `wing` is `None` (global search), `include_local` is always `true`
    /// and the remote targets are the deduped, name-ordered set of every remote
    /// referenced by a non-Local rule: the default route (if non-Local) plus every
    /// wing entry in `self.rules.wings` whose mode is non-Local. Only remotes
    /// actually present in `self.remotes` are included.
    pub fn plan_search_targets(
        &self,
        wing: Option<&str>,
        room: Option<&str>,
    ) -> (bool, Vec<String>) {
        // ── Diary guard ───────────────────────────────────────────────────────
        if wing == Some(SHARED_AGENT_DIARY_WING) || room == Some(DIARY_ROOM) {
            return (true, vec![]);
        }

        if let Some(w) = wing {
            // ── Specific wing ─────────────────────────────────────────────────
            let rule = resolve_route(
                &self.rules,
                None,
                RouteQuery { wing: Some(w), room, source_file: None },
            );
            match rule.mode {
                RouteMode::Local => (true, vec![]),
                RouteMode::Remote => {
                    let targets = rule
                        .remote
                        .as_ref()
                        .filter(|name| self.remotes.contains_key(*name))
                        .map(|name| vec![name.clone()])
                        .unwrap_or_default();
                    (false, targets)
                }
                RouteMode::Combined => {
                    let targets = rule
                        .remote
                        .as_ref()
                        .filter(|name| self.remotes.contains_key(*name))
                        .map(|name| vec![name.clone()])
                        .unwrap_or_default();
                    (true, targets)
                }
            }
        } else {
            // ── Global search: include_local always true ───────────────────────
            // Collect all non-Local remote names: default route + all wing rules.
            let mut target_set = std::collections::BTreeSet::new();

            // Default route.
            let default_rule = resolve_route(
                &self.rules,
                None,
                RouteQuery { wing: None, room, source_file: None },
            );
            if default_rule.mode != RouteMode::Local {
                if let Some(name) = &default_rule.remote {
                    if self.remotes.contains_key(name) {
                        target_set.insert(name.clone());
                    }
                }
            }

            // Per-wing rules.
            for rule in self.rules.wings.values() {
                if rule.mode != RouteMode::Local {
                    if let Some(name) = &rule.remote {
                        if self.remotes.contains_key(name) {
                            target_set.insert(name.clone());
                        }
                    }
                }
            }

            (true, target_set.into_iter().collect())
        }
    }

    /// Fan out search to one or more remotes concurrently and merge with local
    /// results. `remote_targets` is the list of remote names to query; when empty
    /// the local results are returned unchanged.
    ///
    /// Reads never hard-fail on remote outage.
    pub async fn search(
        &self,
        local_results: Vec<Value>,
        query: &str,
        wing: Option<&str>,
        room: Option<&str>,
        view: Option<&str>,
        limit: usize,
        remote_targets: &[String],
    ) -> ToolResult<Value> {
        if remote_targets.is_empty() {
            return Ok(search_payload(query, wing, room, local_results, &[], &[]));
        }

        // Fan out to all target remotes concurrently.
        let mut set: JoinSet<(String, Result<Vec<Value>, mempalace_remote::RemoteError>)> =
            JoinSet::new();
        for name in remote_targets {
            let name = name.clone();
            let query_str = query.to_owned();
            let wing_owned = wing.map(|s| s.to_owned());
            let room_owned = room.map(|s| s.to_owned());
            let view_owned = view.map(|s| s.to_owned());
            let api = match self.remotes.get(&name) {
                Some(a) => Arc::clone(a),
                None => continue,
            };
            set.spawn(async move {
                let req = DrawerSearchRequest {
                    query: query_str,
                    wing: wing_owned,
                    room: room_owned,
                    limit: Some(limit),
                    view: view_owned,
                };
                match api.search_drawers(req).await {
                    Ok(response) => {
                        let results = response
                            .results
                            .into_iter()
                            .map(|r| drawer_result_to_value(r, &name))
                            .collect();
                        (name, Ok(results))
                    }
                    Err(e) => (name.clone(), Err(e)),
                }
            });
        }

        // Collect results in deterministic name order.
        let mut remote_results_by_name: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        let mut warnings: Vec<String> = Vec::new();
        let mut degradations: Vec<Value> = Vec::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(results))) => {
                    remote_results_by_name.insert(name, results);
                }
                Ok((name, Err(error))) => {
                    tracing::warn!(remote = %name, %error, "search degraded");
                    warnings.push(format!("remote `{name}` search failed: {error}"));
                    degradations.push(structured_degradation(&name, "search", &error));
                }
                Err(join_err) => {
                    tracing::warn!("search task panicked: {join_err}");
                }
            }
        }

        // N-way interleave: local first at each rank, then remotes in name order.
        let mut all_origins: Vec<(String, Vec<Value>)> = Vec::new();
        if !local_results.is_empty() {
            all_origins.push(("local".to_owned(), local_results));
        }
        for (name, results) in remote_results_by_name {
            all_origins.push((name, results));
        }

        let merged = merge_search_results_nway(all_origins, limit);
        Ok(search_payload(query, wing, room, merged, &warnings, &degradations))
    }

    // ─── Add drawer ──────────────────────────────────────────────────────────────

    /// Route an add-drawer operation to a remote server.
    ///
    /// Returns `Some(remote_result)` when the write is routed to a remote;
    /// `None` when the caller should execute locally.
    ///
    /// - `Local` → returns `Ok(None)`
    /// - `Remote` → sends the add to the configured remote
    /// - `Combined + write:remote` → sends the add to the configured remote
    /// - `Combined + write:both` → returns `None` (defensive; callers should skip)
    ///
    /// **Diary guard:** Diary-shaped drawers (`wing == SHARED_AGENT_DIARY_WING`,
    /// `room == DIARY_ROOM`, or `source_file` starting with
    /// `DIARY_TOPIC_PREFIX`) are never written remotely — returns `Ok(None)`
    /// immediately.
    ///
    /// Pre-add duplicate check is performed before posting the add request; if
    /// the duplicate check itself fails (transport error) a warning is emitted
    /// and the add proceeds (the server re-checks anyway).
    pub async fn add_drawer_remote(
        &self,
        wing: &str,
        room: &str,
        content: &str,
        source_file: &str,
        added_by: &str,
        route: &ResolvedRouteRule,
        duplicate_threshold: f32,
    ) -> ToolResult<Option<Value>> {
        self.add_drawer_remote_with_operation(
            wing,
            room,
            content,
            source_file,
            added_by,
            route,
            duplicate_threshold,
            None,
        )
        .await
    }

    pub async fn add_drawer_remote_with_operation(
        &self,
        wing: &str,
        room: &str,
        content: &str,
        source_file: &str,
        added_by: &str,
        route: &ResolvedRouteRule,
        duplicate_threshold: f32,
        operation_id: Option<&str>,
    ) -> ToolResult<Option<Value>> {
        // ── Diary guard: diary-shaped drawers never write remotely ──────────
        if wing == SHARED_AGENT_DIARY_WING
            || room == DIARY_ROOM
            || source_file.starts_with(DIARY_TOPIC_PREFIX)
        {
            return Ok(None);
        }

        // ── Resolve the target remote for remote-only writes ───────────────
        // Dual-write (Both) routes are handled by add_drawer_replicate and
        // should not reach this method. We keep Both as a defensive arm.
        let target_remote = match route.mode {
            RouteMode::Local => None,
            RouteMode::Remote => route.remote.as_deref(),
            RouteMode::Combined => match route.write {
                WriteTarget::Local => None,
                WriteTarget::Remote => route.remote.as_deref(),
                // Defensive: caller should have filtered Both via is_dual_write.
                WriteTarget::Both => None,
            },
        };
        let Some(remote_name) = target_remote else {
            return Ok(None);
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(None);
        };

        // ── Pre-add duplicate check ───────────────────────────────────────────
        // Operation-aware retries carry a stable operation_id, so the client-side
        // semantic preflight must be bypassed entirely: the receiving receipt store
        // authoritatively replays the mutation. Running the preflight on a retry whose
        // first attempt committed but lost its response would short-circuit on the
        // duplicate and never reach the server receipt replay, recreating the ambiguous
        //-outcome incident #127 exists to fix. The preflight stays for legacy calls that
        // carry no operation id.
        if operation_id.is_none() {
            let pre_check_req = mempalace_federation::CheckDuplicateRequest {
                content: content.to_owned(),
                threshold: Some(duplicate_threshold),
            };
            match api.check_duplicate(pre_check_req).await {
                Ok(resp) if resp.is_duplicate => {
                    let mut matches = resp.matches.as_array().cloned().unwrap_or_default();
                    for m in &mut matches {
                        if let Some(obj) = m.as_object_mut() {
                            obj.insert("origin".to_owned(), json!(remote_name));
                        }
                    }
                    return Ok(Some(json!({
                        "success": false,
                        "reason": "duplicate",
                        "matches": matches,
                        "origin": remote_name,
                        "applied_to": format_remote_origin(remote_name),
                    })));
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(
                        remote = %remote_name,
                        "pre-add duplicate check failed (proceeding with add): {e}"
                    );
                }
            };
        }

        let generated_op_id;
        let effective_operation_id = match operation_id {
            Some(id) => id,
            None => {
                generated_op_id = generate_operation_id();
                generated_op_id.as_str()
            }
        };

        let req = AddDrawerRequest {
            wing: wing.to_owned(),
            room: room.to_owned(),
            content: content.to_owned(),
            source_file: if source_file.is_empty() { None } else { Some(source_file.to_owned()) },
            added_by: Some(added_by.to_owned()),
            drawer_id: None,
            operation_id: Some(effective_operation_id.to_owned()),
        };
        match api.add_drawer(req).await {
            Ok(resp) => {
                // resp.success is true on 2xx; keep a defensive branch just in case.
                if resp.success {
                    Ok(Some(json!({
                        "success": true,
                        "drawer_id": resp.drawer_id,
                        "wing": resp.wing,
                        "room": resp.room,
                        "origin": remote_name,
                        "applied_to": format_remote_origin(remote_name),
                    })))
                } else {
                    // Defensive: server indicated failure on 2xx (shouldn't happen in v1).
                    Ok(Some(json!({
                        "success": false,
                        "reason": "rejected",
                        "origin": remote_name,
                        "applied_to": format_remote_origin(remote_name),
                    })))
                }
            }
            Err(error @ RemoteError::RemoteRejected { status: 409, .. })
                if is_duplicate_rejection(&error) =>
            {
                // Race condition: duplicate inserted between pre-check and add.  A different
                // 409 (notably operation_id_conflict) is authoritative and must not be hidden as
                // a harmless duplicate.
                Ok(Some(json!({
                    "success": false,
                    "reason": "duplicate",
                    "matches": [],
                    "origin": remote_name,
                    "applied_to": format_remote_origin(remote_name),
                })))
            }
            Err(e) if e.is_unknown_outcome() => {
                Ok(Some(remote_mutation_unknown_outcome_value(&e, effective_operation_id)))
            }
            Err(e) => Err(ToolError::Internal(McpError::Federation(format!(
                "remote `{remote_name}` add_drawer failed: {e}"
            )))),
        }
    }

    /// Best-effort remote replication for [`WriteTarget::Both`] after the
    /// local write has already completed.  Attempts a pre-add duplicate check
    /// first; if it hits a duplicate or transport failure, logs a warning and
    /// returns [`ReplicationStatus::Failed`].  On success returns
    /// [`ReplicationStatus::Replicated`].  Never blocks the caller from the
    /// local write path.
    ///
    /// **Diary guard:** Diary-shaped drawers (`wing == SHARED_AGENT_DIARY_WING`,
    /// `room == DIARY_ROOM`, or `source_file` starting with
    /// `DIARY_TOPIC_PREFIX`) are never replicated remotely — returns
    /// [`ReplicationStatus::Skipped`] immediately.
    pub async fn add_drawer_replicate(
        &self,
        wing: &str,
        room: &str,
        content: &str,
        source_file: &str,
        added_by: &str,
        route: &ResolvedRouteRule,
        duplicate_threshold: f32,
    ) -> ReplicationStatus {
        // ── Diary guard: diary-shaped drawers never replicate ──────────────
        if wing == SHARED_AGENT_DIARY_WING
            || room == DIARY_ROOM
            || source_file.starts_with(DIARY_TOPIC_PREFIX)
        {
            return ReplicationStatus::Skipped;
        }

        let remote_name = match &route.write {
            WriteTarget::Both => route.remote.as_deref(),
            _ => return ReplicationStatus::Skipped,
        };
        let Some(remote_name) = remote_name else {
            return ReplicationStatus::Failed {
                remote: "(none)".to_owned(),
                reason: "write:both route has no remote configured".to_owned(),
            };
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return ReplicationStatus::Failed {
                remote: remote_name.to_owned(),
                reason: "no client available for remote".to_owned(),
            };
        };

        // ── Pre-add duplicate check (best-effort) ─────────────────────────
        let pre_check_req = mempalace_federation::CheckDuplicateRequest {
            content: content.to_owned(),
            threshold: Some(duplicate_threshold),
        };
        match api.check_duplicate(pre_check_req).await {
            Ok(resp) if resp.is_duplicate => {
                let content_hash = hash_text(content);
                let is_exact_match = resp.matches.as_array().map_or(false, |arr| {
                    arr.iter().any(|m| {
                        m.get("content_hash")
                            .and_then(|v| v.as_str())
                            .map_or(false, |h| h == content_hash)
                    })
                });

                if is_exact_match {
                    tracing::info!(
                        remote = %remote_name,
                        wing = %wing,
                        room = %room,
                        "add_drawer replicate: exact content already exists remotely; converged"
                    );
                    return ReplicationStatus::Converged { remote: remote_name.to_owned() };
                }

                tracing::warn!(
                    remote = %remote_name,
                    wing = %wing,
                    room = %room,
                    "add_drawer replicate: duplicate exists remotely; skipping add"
                );
                return ReplicationStatus::Failed {
                    remote: remote_name.to_owned(),
                    reason: "duplicate exists on remote".to_owned(),
                };
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    remote = %remote_name,
                    "add_drawer replicate: pre-check failed: {e}"
                );
                return ReplicationStatus::Failed {
                    remote: remote_name.to_owned(),
                    reason: format!("pre-check failed: {e}"),
                };
            }
        };

        let req = AddDrawerRequest {
            wing: wing.to_owned(),
            room: room.to_owned(),
            content: content.to_owned(),
            source_file: if source_file.is_empty() { None } else { Some(source_file.to_owned()) },
            added_by: Some(added_by.to_owned()),
            drawer_id: None,
            operation_id: None,
        };
        match api.add_drawer(req).await {
            Ok(resp) if resp.success => {
                tracing::info!(
                    remote = %remote_name,
                    wing = %wing,
                    room = %room,
                    "add_drawer replicate: remote write succeeded"
                );
                ReplicationStatus::Replicated { remote: remote_name.to_owned() }
            }
            Ok(_) => {
                tracing::warn!(
                    remote = %remote_name,
                    wing = %wing,
                    room = %room,
                    "add_drawer replicate: remote rejected the write"
                );
                ReplicationStatus::Failed {
                    remote: remote_name.to_owned(),
                    reason: "remote rejected the write".to_owned(),
                }
            }
            Err(error @ RemoteError::RemoteRejected { status: 409, .. }) => {
                if is_duplicate_rejection(&error) {
                    tracing::warn!(
                        remote = %remote_name,
                        wing = %wing,
                        room = %room,
                        "add_drawer replicate: remote duplicate (409)"
                    );
                    ReplicationStatus::Failed {
                        remote: remote_name.to_owned(),
                        reason: "duplicate (409) on remote".to_owned(),
                    }
                } else {
                    tracing::warn!(
                        remote = %remote_name,
                        wing = %wing,
                        room = %room,
                        error = %error,
                        "add_drawer replicate: remote rejected the write"
                    );
                    ReplicationStatus::Failed {
                        remote: remote_name.to_owned(),
                        reason: format!("remote rejection: {error}"),
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    remote = %remote_name,
                    "add_drawer replicate: transport failure: {e}"
                );
                ReplicationStatus::Failed {
                    remote: remote_name.to_owned(),
                    reason: format!("transport failure: {e}"),
                }
            }
        }
    }

    // ─── Check duplicate ─────────────────────────────────────────────────────────

    /// Fan out duplicate check to all configured remotes in parallel, merging
    /// results with origin annotation.
    pub async fn check_duplicate_all_remotes(&self, content: &str, threshold: f32) -> Vec<Value> {
        if self.remotes.is_empty() {
            return vec![];
        }
        let mut set = JoinSet::new();
        for (name, api) in &self.remotes {
            let name = name.clone();
            let content = content.to_owned();
            let api = Arc::clone(api);
            set.spawn(async move {
                let req = mempalace_federation::CheckDuplicateRequest {
                    content,
                    threshold: Some(threshold),
                };
                match api.check_duplicate(req).await {
                    Ok(resp) => {
                        let matches = resp.matches.as_array().cloned().unwrap_or_default();
                        matches
                            .into_iter()
                            .map(|mut m| {
                                if let Some(obj) = m.as_object_mut() {
                                    obj.insert("origin".to_owned(), json!(name));
                                }
                                m
                            })
                            .collect::<Vec<_>>()
                    }
                    Err(_) => vec![],
                }
            });
        }
        let mut results = vec![];
        while let Some(res) = set.join_next().await {
            match res {
                Ok(batch) => results.extend(batch),
                Err(join_err) => {
                    tracing::warn!("check_duplicate task panicked: {join_err}");
                }
            }
        }
        results
    }

    // ─── Delete drawer ───────────────────────────────────────────────────────────

    pub async fn delete_drawer_routed_remote(
        &self,
        drawer_id: &str,
        route: &ResolvedRouteRule,
        operation_id: Option<&str>,
    ) -> ToolResult<Option<Value>> {
        let remote_name = match self.resolve_write_target(route) {
            WriteTarget::Remote => route.remote.as_deref(),
            WriteTarget::Local | WriteTarget::Both => None,
        };
        let Some(remote_name) = remote_name else {
            return Ok(None);
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Err(ToolError::Internal(McpError::Federation(format!(
                "no client available for remote `{remote_name}`"
            ))));
        };
        let generated_op_id;
        let effective_operation_id = match operation_id {
            Some(id) => id,
            None => {
                generated_op_id = generate_operation_id();
                generated_op_id.as_str()
            }
        };
        match api.delete_drawer_with_operation_id(drawer_id, Some(effective_operation_id)).await {
            Ok(()) => Ok(Some(json!({
                "success": true,
                "drawer_id": drawer_id,
                "origin": remote_name,
                "applied_to": format_remote_origin(remote_name),
            }))),
            Err(e) if e.is_unknown_outcome() => {
                Ok(Some(remote_mutation_unknown_outcome_value(&e, effective_operation_id)))
            }
            Err(e) => Err(ToolError::Internal(McpError::Federation(format!(
                "remote `{remote_name}` delete_drawer failed: {e}"
            )))),
        }
    }

    /// Try to delete a drawer from ALL configured remotes in config order (BTreeMap
    /// name order — deterministic). Returns the first success with "origin".
    /// Deletion is by drawer id; the wing is not known here.
    pub async fn delete_drawer_remote(&self, drawer_id: &str) -> ToolResult<Option<Value>> {
        self.delete_drawer_remote_with_operation(drawer_id, None).await
    }

    pub async fn delete_drawer_remote_with_operation(
        &self,
        drawer_id: &str,
        operation_id: Option<&str>,
    ) -> ToolResult<Option<Value>> {
        let generated_op_id;
        let effective_operation_id = match operation_id {
            Some(id) => id,
            None => {
                generated_op_id = generate_operation_id();
                generated_op_id.as_str()
            }
        };
        for (name, api) in &self.remotes {
            match api.delete_drawer_with_operation_id(drawer_id, Some(effective_operation_id)).await
            {
                Ok(()) => {
                    return Ok(Some(json!({
                        "success": true,
                        "drawer_id": drawer_id,
                        "origin": name,
                        "applied_to": format_remote_origin(name),
                    })));
                }
                // The request may have committed but its outcome is unconfirmed: surface the
                // ambiguity honestly instead of swallowing it into a false not-found.
                Err(e) if e.is_unknown_outcome() => {
                    return Ok(Some(remote_mutation_unknown_outcome_value(
                        &e,
                        effective_operation_id,
                    )));
                }
                Err(e) if e.is_degradable() => continue,
                Err(e) => {
                    tracing::warn!(
                        remote = %name,
                        error = %e,
                        "non-degradable error during remote drawer delete"
                    );
                    continue;
                }
            }
        }
        Ok(None)
    }

    // ─── Taxonomy / Status ───────────────────────────────────────────────────────

    /// Fan out taxonomy queries to all remotes concurrently, merging into the
    /// local payload. Wing-collision warnings are emitted when a wing appears
    /// both locally and on a remote but its resolved route is Local-only.
    pub async fn taxonomy_merge(&self, local_taxonomy: Value) -> ToolResult<Value> {
        let mut set: JoinSet<(String, Result<Value, mempalace_remote::RemoteError>)> =
            JoinSet::new();
        for (name, api) in &self.remotes {
            let name = name.clone();
            let api = Arc::clone(api);
            set.spawn(async move { (name, api.taxonomy().await) });
        }

        let mut remote_results: BTreeMap<String, Value> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(payload))) => {
                    remote_results.insert(name, payload);
                }
                Ok((name, Err(e))) => {
                    tracing::warn!(remote = %name, %e, "failed to fetch taxonomy from remote");
                }
                Err(join_err) => {
                    tracing::warn!("taxonomy task panicked: {join_err}");
                }
            }
        }

        let mut merged = local_taxonomy;
        for (_name, remote) in &remote_results {
            if let Some(remote_taxonomy) = remote.get("taxonomy") {
                if let (Some(obj), Some(robj)) = (
                    merged.get_mut("taxonomy").and_then(|v| v.as_object_mut()),
                    remote_taxonomy.as_object(),
                ) {
                    for (wing, rooms) in robj {
                        if let Some(rooms_obj) = rooms.as_object() {
                            let wing_entry = obj.entry(wing.clone()).or_insert_with(|| json!({}));
                            if let Some(wing_map) = wing_entry.as_object_mut() {
                                for (room, count) in rooms_obj {
                                    let c = count.as_u64().unwrap_or(0);
                                    let entry = wing_map.entry(room.clone()).or_insert(json!(0));
                                    let val = entry.as_u64().unwrap_or(0);
                                    *entry = json!(val + c);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(merged)
    }

    pub async fn wings_merge(&self, local_wings: Value) -> ToolResult<Value> {
        let mut set: JoinSet<(String, Result<Value, mempalace_remote::RemoteError>)> =
            JoinSet::new();
        for (name, api) in &self.remotes {
            let name = name.clone();
            let api = Arc::clone(api);
            set.spawn(async move { (name, api.wings().await) });
        }

        let mut remote_results: BTreeMap<String, Value> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(payload))) => {
                    remote_results.insert(name, payload);
                }
                Ok((name, Err(e))) => {
                    tracing::warn!(remote = %name, %e, "failed to fetch wings from remote");
                }
                Err(join_err) => {
                    tracing::warn!("wings task panicked: {join_err}");
                }
            }
        }

        // Collect local wing names for collision detection.
        let local_wing_names: std::collections::BTreeSet<String> = local_wings
            .get("wings")
            .and_then(|v| v.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();

        let mut merged = local_wings;
        for (remote_name, remote) in &remote_results {
            if let Some(remote_wings) = remote.get("wings") {
                if let (Some(obj), Some(robj)) = (
                    merged.get_mut("wings").and_then(|v| v.as_object_mut()),
                    remote_wings.as_object(),
                ) {
                    for (wing, count) in robj {
                        // Wing-collision warning: wing exists locally AND on remote
                        // but the resolved route is Local-only.
                        if local_wing_names.contains(wing) {
                            let rule = resolve_route(
                                &self.rules,
                                None,
                                RouteQuery {
                                    wing: Some(wing.as_str()),
                                    room: None,
                                    source_file: None,
                                },
                            );
                            if rule.mode == RouteMode::Local {
                                tracing::warn!(
                                    wing = %wing,
                                    remote = %remote_name,
                                    "wing exists on remote but is configured local-only; results stay split"
                                );
                            }
                        }
                        let c = count.as_u64().unwrap_or(0);
                        let entry = obj.entry(wing.clone()).or_insert(json!(0));
                        let val = entry.as_u64().unwrap_or(0);
                        *entry = json!(val + c);
                    }
                }
            }
        }
        Ok(merged)
    }

    pub async fn rooms_merge(
        &self,
        local_rooms: Value,
        wing_filter: Option<&str>,
    ) -> ToolResult<Value> {
        let wing_filter_owned = wing_filter.map(|s| s.to_owned());
        let mut set: JoinSet<(String, Result<Value, mempalace_remote::RemoteError>)> =
            JoinSet::new();
        for (name, api) in &self.remotes {
            let name = name.clone();
            let api = Arc::clone(api);
            let wf = wing_filter_owned.clone();
            set.spawn(async move { (name, api.rooms(wf.as_deref()).await) });
        }

        let mut remote_results: BTreeMap<String, Value> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(payload))) => {
                    remote_results.insert(name, payload);
                }
                Ok((name, Err(e))) => {
                    tracing::warn!(remote = %name, %e, "failed to fetch rooms from remote");
                }
                Err(join_err) => {
                    tracing::warn!("rooms task panicked: {join_err}");
                }
            }
        }

        let mut merged = local_rooms;
        for (_name, remote) in &remote_results {
            if let Some(remote_rooms) = remote.get("rooms") {
                if let (Some(obj), Some(robj)) = (
                    merged.get_mut("rooms").and_then(|v| v.as_object_mut()),
                    remote_rooms.as_object(),
                ) {
                    for (room, count) in robj {
                        let c = count.as_u64().unwrap_or(0);
                        let entry = obj.entry(room.clone()).or_insert(json!(0));
                        let val = entry.as_u64().unwrap_or(0);
                        *entry = json!(val + c);
                    }
                }
            }
        }
        Ok(merged)
    }

    pub async fn status_merge(&self, mut local_status: Value) -> ToolResult<Value> {
        let mut set: JoinSet<(
            String,
            String,
            Result<mempalace_federation::InfoResponse, mempalace_remote::RemoteError>,
        )> = JoinSet::new();
        for (name, api) in &self.remotes {
            let name = name.clone();
            let url = self.rules.remotes.get(&name).map(|r| r.url.clone()).unwrap_or_default();
            let api = Arc::clone(api);
            set.spawn(async move { (name, url, api.info().await) });
        }

        let mut info_results: BTreeMap<
            String,
            (String, Result<mempalace_federation::InfoResponse, mempalace_remote::RemoteError>),
        > = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, url, result)) => {
                    info_results.insert(name, (url, result));
                }
                Err(join_err) => {
                    tracing::warn!("status task panicked: {join_err}");
                }
            }
        }

        let mut federation_info = vec![];
        for (name, (url, result)) in &info_results {
            let mut entry = json!({
                "name": name,
                "url": url,
                "reachable": false,
                "federation_api_version": null,
            });
            if let Ok(info) = result {
                entry["reachable"] = json!(true);
                entry["federation_api_version"] = json!(info.federation_api_version);
            }
            federation_info.push(entry);
        }
        if let Some(obj) = local_status.as_object_mut() {
            obj.insert("federation".to_owned(), json!({ "remotes": federation_info }));
        }
        Ok(local_status)
    }

    // ─── Changes feed ────────────────────────────────────────────────────────────

    /// Fan out a changes query to ALL configured remotes concurrently.
    ///
    /// Returns a map of `remote_name → per-remote result`. On success the value
    /// is `{ "events": [...], "next_cursor": <string|null> }` where each event
    /// object carries an added `"origin"` field (`"remote:<name>"`). On any
    /// transport or application error (including join panics) the value is
    /// `{ "unreachable": true, "error": "<message>" }`.
    ///
    /// A down remote NEVER poisons healthy remotes.
    pub async fn changes_fanout(
        &self,
        since: Option<String>,
        limit: Option<usize>,
        cursors: &BTreeMap<String, String>,
    ) -> BTreeMap<String, Value> {
        let mut set: JoinSet<(String, Result<Value, String>)> = JoinSet::new();

        for (name, api) in &self.remotes {
            let name = name.clone();
            let api = Arc::clone(api);
            let query =
                ChangesQuery { since: since.clone(), limit, cursor: cursors.get(&name).cloned() };
            set.spawn(async move {
                match api.changes(query).await {
                    Ok(resp) => {
                        let origin = format_remote_origin(&name);
                        let events: Vec<Value> = resp
                            .events
                            .into_iter()
                            .map(|evt| {
                                let mut v =
                                    serde_json::to_value(&evt).unwrap_or_else(|_| json!({}));
                                if let Some(obj) = v.as_object_mut() {
                                    obj.insert("origin".to_owned(), json!(origin));
                                }
                                v
                            })
                            .collect();
                        let payload = json!({
                            "events": events,
                            "next_cursor": resp.next_cursor,
                        });
                        (name, Ok(payload))
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        (name, Err(msg))
                    }
                }
            });
        }

        let mut results: BTreeMap<String, Value> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(payload))) => {
                    results.insert(name, payload);
                }
                Ok((name, Err(msg))) => {
                    tracing::warn!(remote = %name, "changes fan-out failed: {msg}");
                    results.insert(name, json!({ "unreachable": true, "error": msg }));
                }
                Err(join_err) => {
                    tracing::warn!("changes fan-out task panicked: {join_err}");
                }
            }
        }

        results
    }

    // ─── KG ──────────────────────────────────────────────────────────────────────

    pub async fn kg_query_merge(
        &self,
        local_payload: Value,
        entity: &str,
        route: &ResolvedRouteRule,
    ) -> ToolResult<Value> {
        let remote_name = match route.mode {
            RouteMode::Local => return Ok(local_payload),
            RouteMode::Remote | RouteMode::Combined => route.remote.as_deref().unwrap_or("remote"),
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(local_payload);
        };
        let as_of = local_payload["as_of"].as_str().map(|s| s.to_owned());
        let req = mempalace_federation::KgQueryRequest {
            entity: entity.to_owned(),
            as_of,
            direction: None,
        };
        let remote_payload = match api.kg_query(req).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!(
                    remote = %remote_name,
                    "kg_query failed: {e}"
                );
                return Ok(degraded_read_payload(local_payload, remote_name, "kg_query", &e));
            }
        };
        match route.mode {
            RouteMode::Local => unreachable!(),
            RouteMode::Remote => {
                let mut payload = remote_payload;
                annotate_kg_facts_origin(&mut payload, remote_name);
                Ok(payload)
            }
            RouteMode::Combined => Ok(merge_kg_facts(local_payload, remote_payload, remote_name)),
        }
    }

    pub async fn kg_add_remote(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        valid_from: Option<&str>,
        route: &ResolvedRouteRule,
    ) -> ToolResult<Option<Value>> {
        self.kg_add_remote_with_operation(subject, predicate, object, valid_from, route, None).await
    }

    pub async fn kg_add_remote_with_operation(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        valid_from: Option<&str>,
        route: &ResolvedRouteRule,
        operation_id: Option<&str>,
    ) -> ToolResult<Option<Value>> {
        // ── Resolve the target remote for remote-only writes ───────────────
        // Dual-write (Both) routes are handled by kg_add_replicate and
        // should not reach this method. We keep Both as a defensive arm.
        let target_remote = match route.mode {
            RouteMode::Local => None,
            RouteMode::Remote => route.remote.as_deref(),
            RouteMode::Combined => match route.write {
                WriteTarget::Local => None,
                WriteTarget::Remote => route.remote.as_deref(),
                // Defensive: caller should have filtered Both via is_dual_write.
                WriteTarget::Both => None,
            },
        };
        let Some(remote_name) = target_remote else {
            return Ok(None);
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(None);
        };
        let generated_op_id;
        let effective_operation_id = match operation_id {
            Some(id) => id,
            None => {
                generated_op_id = generate_operation_id();
                generated_op_id.as_str()
            }
        };
        let req = mempalace_federation::KgAddFactRequest {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: object.to_owned(),
            valid_from: valid_from.map(|s| s.to_owned()),
            operation_id: Some(effective_operation_id.to_owned()),
        };
        match api.kg_add_fact(req).await {
            Ok(mut resp) => {
                if let Some(obj) = resp.as_object_mut() {
                    obj.insert("applied_to".to_owned(), json!(format_remote_origin(remote_name)));
                }
                Ok(Some(resp))
            }
            Err(e) if e.is_unknown_outcome() => {
                Ok(Some(remote_mutation_unknown_outcome_value(&e, effective_operation_id)))
            }
            Err(e) => Err(ToolError::Internal(McpError::Federation(format!(
                "remote `{remote_name}` kg_add_fact failed: {e}"
            )))),
        }
    }

    /// Best-effort remote KG fact replication for [`WriteTarget::Both`] after
    /// the local KG write has already completed.
    pub async fn kg_add_replicate(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        valid_from: Option<&str>,
        route: &ResolvedRouteRule,
    ) -> ReplicationStatus {
        let remote_name = match &route.write {
            WriteTarget::Both => route.remote.as_deref(),
            _ => return ReplicationStatus::Skipped,
        };
        let Some(remote_name) = remote_name else {
            return ReplicationStatus::Failed {
                remote: "(none)".to_owned(),
                reason: "write:both route has no remote configured".to_owned(),
            };
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return ReplicationStatus::Failed {
                remote: remote_name.to_owned(),
                reason: "no client available for remote".to_owned(),
            };
        };
        let req = mempalace_federation::KgAddFactRequest {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: object.to_owned(),
            valid_from: valid_from.map(|s| s.to_owned()),
            operation_id: None,
        };
        match api.kg_add_fact(req).await {
            Ok(_) => {
                tracing::info!(
                    remote = %remote_name,
                    subject = %subject,
                    predicate = %predicate,
                    object = %object,
                    "kg_add replicate: remote write succeeded"
                );
                ReplicationStatus::Replicated { remote: remote_name.to_owned() }
            }
            Err(e) => {
                tracing::warn!(
                    remote = %remote_name,
                    subject = %subject,
                    predicate = %predicate,
                    object = %object,
                    "kg_add replicate: remote failed: {e}"
                );
                ReplicationStatus::Failed { remote: remote_name.to_owned(), reason: format!("{e}") }
            }
        }
    }

    pub async fn kg_invalidate_remote(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        ended: Option<&str>,
        route: &ResolvedRouteRule,
    ) -> ToolResult<Option<Value>> {
        self.kg_invalidate_remote_with_operation(subject, predicate, object, ended, route, None)
            .await
    }

    pub async fn kg_invalidate_remote_with_operation(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        ended: Option<&str>,
        route: &ResolvedRouteRule,
        operation_id: Option<&str>,
    ) -> ToolResult<Option<Value>> {
        // ── Resolve the target remote for remote-only writes ───────────────
        // Dual-write (Both) routes are handled by kg_invalidate_replicate and
        // should not reach this method. We keep Both as a defensive arm.
        let target_remote = match route.mode {
            RouteMode::Local => None,
            RouteMode::Remote => route.remote.as_deref(),
            RouteMode::Combined => match route.write {
                WriteTarget::Local => None,
                WriteTarget::Remote => route.remote.as_deref(),
                // Defensive: caller should have filtered Both via is_dual_write.
                WriteTarget::Both => None,
            },
        };
        let Some(remote_name) = target_remote else {
            return Ok(None);
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(None);
        };
        let generated_op_id;
        let effective_operation_id = match operation_id {
            Some(id) => id,
            None => {
                generated_op_id = generate_operation_id();
                generated_op_id.as_str()
            }
        };
        let req = mempalace_federation::KgInvalidateRequest {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: object.to_owned(),
            ended: ended.map(|s| s.to_owned()),
            operation_id: Some(effective_operation_id.to_owned()),
        };
        match api.kg_invalidate(req).await {
            Ok(mut resp) => {
                if let Some(obj) = resp.as_object_mut() {
                    obj.insert("applied_to".to_owned(), json!(format_remote_origin(remote_name)));
                }
                Ok(Some(resp))
            }
            Err(e) if e.is_unknown_outcome() => {
                Ok(Some(remote_mutation_unknown_outcome_value(&e, effective_operation_id)))
            }
            Err(e) => Err(ToolError::Internal(McpError::Federation(format!(
                "remote `{remote_name}` kg_invalidate failed: {e}"
            )))),
        }
    }

    /// Best-effort remote KG invalidation replication for [`WriteTarget::Both`]
    /// after the local KG invalidation has already completed.
    pub async fn kg_invalidate_replicate(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        ended: Option<&str>,
        route: &ResolvedRouteRule,
    ) -> ReplicationStatus {
        let remote_name = match &route.write {
            WriteTarget::Both => route.remote.as_deref(),
            _ => return ReplicationStatus::Skipped,
        };
        let Some(remote_name) = remote_name else {
            return ReplicationStatus::Failed {
                remote: "(none)".to_owned(),
                reason: "write:both route has no remote configured".to_owned(),
            };
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return ReplicationStatus::Failed {
                remote: remote_name.to_owned(),
                reason: "no client available for remote".to_owned(),
            };
        };
        let req = mempalace_federation::KgInvalidateRequest {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: object.to_owned(),
            ended: ended.map(|s| s.to_owned()),
            operation_id: None,
        };
        match api.kg_invalidate(req).await {
            Ok(_) => {
                tracing::info!(
                    remote = %remote_name,
                    subject = %subject,
                    predicate = %predicate,
                    object = %object,
                    "kg_invalidate replicate: remote write succeeded"
                );
                ReplicationStatus::Replicated { remote: remote_name.to_owned() }
            }
            Err(e) => {
                tracing::warn!(
                    remote = %remote_name,
                    subject = %subject,
                    predicate = %predicate,
                    object = %object,
                    "kg_invalidate replicate: remote failed: {e}"
                );
                ReplicationStatus::Failed { remote: remote_name.to_owned(), reason: format!("{e}") }
            }
        }
    }

    pub async fn kg_timeline_merge(
        &self,
        local_payload: Value,
        entity: Option<&str>,
        route: &ResolvedRouteRule,
    ) -> ToolResult<Value> {
        let remote_name = match route.mode {
            RouteMode::Local => return Ok(local_payload),
            RouteMode::Remote | RouteMode::Combined => route.remote.as_deref().unwrap_or("remote"),
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(local_payload);
        };
        let remote_payload = match api.kg_timeline(entity).await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!(
                    remote = %remote_name,
                    "kg_timeline failed: {e}"
                );
                return Ok(degraded_read_payload(local_payload, remote_name, "kg_timeline", &e));
            }
        };
        match route.mode {
            RouteMode::Local => unreachable!(),
            RouteMode::Remote => {
                let mut payload = remote_payload;
                annotate_kg_facts_origin(&mut payload, remote_name);
                Ok(payload)
            }
            RouteMode::Combined => {
                Ok(merge_kg_timeline(local_payload, remote_payload, remote_name))
            }
        }
    }

    pub async fn kg_stats_merge(
        &self,
        local_payload: Value,
        route: &ResolvedRouteRule,
    ) -> ToolResult<Value> {
        let remote_name = match route.mode {
            RouteMode::Local => return Ok(local_payload),
            RouteMode::Remote | RouteMode::Combined => route.remote.as_deref().unwrap_or("remote"),
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(local_payload);
        };
        let remote_payload = match api.kg_stats().await {
            Ok(resp) => resp,
            Err(e) => {
                tracing::warn!(
                    remote = %remote_name,
                    "kg_stats failed: {e}"
                );
                return Ok(degraded_read_payload(local_payload, remote_name, "kg_stats", &e));
            }
        };
        match route.mode {
            RouteMode::Local => unreachable!(),
            RouteMode::Remote => Ok(remote_payload),
            RouteMode::Combined => Ok(merge_kg_stats(local_payload, remote_payload)),
        }
    }

    // ─── Coordination (issue #102 Stage 4) ─────────────────────────────────────
    //
    // Unlike drawers/KG, most coordination operations are keyed by an existing record ID
    // (task_id, message_id, artifact_id, result_id) with no wing supplied in the request at
    // all — `mempalace_task_claim`, `mempalace_message_get`, and friends never take a `wing`
    // argument, only local Get/RevisionedWrite tools already return one. There is therefore no
    // wing to resolve `resolve_coordination_route` against for those calls. `task_create` is
    // the one exception: `NewTaskRequest.wing` is required, so it is the only coordination
    // write that goes through ordinary wing-based route resolution (`resolve_coordination_route`
    // + `resolve_write_target`, mirroring `kg_add_remote` exactly — `write` can only ever be
    // `Local` or `Remote` here, never `Both`, because a coordination route can never resolve to
    // `WriteTarget::Both` — `mempalace-config` rejects that at config load).
    //
    // Every other coordination call — reads and ID-referencing writes alike — is a local-first,
    // ID-discovery fallback: the caller (`McpRuntime` in `lib.rs`) tries local storage first; on
    // a local miss, if any remotes are configured, the methods below try each *candidate* remote
    // (see `coordination_candidate_remotes` — not every configured remote; a remote never wired
    // up for coordination at all is skipped entirely) in name order (`self.remotes` is a
    // `BTreeMap`, so iteration order is already name order) and use whichever one actually has
    // the record — mirroring `delete_drawer_remote`'s existing "local ID lookup, then all
    // remotes in order" pattern, which exists for the exact same reason (no cross-palace ID
    // mapping to route by).
    //
    // Reads and writes deliberately part ways on which errors count as "not this palace, try the
    // next candidate" versus a hard failure — see `coordination_read_fallback` and
    // `coordination_write_fallback`'s doc comments for the exact split and the reasoning behind
    // it. Both agree that `404` and `CapabilityMissing` are skippable: both are the remote
    // giving a definitive, structural "no" (no such record / no coordination support at all),
    // not an ambiguous answer. Where they part ways is everything else: a **write** cannot
    // afford to guess past a remote it could not actually get an answer from (guessing wrong
    // could create a second, divergent record for the same task on the wrong palace), so
    // anything beyond `404`/`CapabilityMissing` — including an unreachable remote — is terminal.
    // A plain **read** has no such downside, so it also skips a genuinely degradable
    // `Unreachable` remote (the federation-wide "reads degrade" rule — see
    // `docs/Federation.md`), but still surfaces `Unauthorized`/`VersionSkew`/malformed-response
    // errors as hard failures: those cannot be read as a definitive answer (the token might be
    // wrong, or the remote's protocol version cannot be trusted), so silently treating them as
    // "record not found" would hide a real misconfiguration from the caller.

    /// Remote names that can actually participate in coordination discovery: every remote
    /// referenced by an explicit `federation.coordination[wing]` rule (there may be several,
    /// one per wing, naming different remotes), plus `default_remote` when coordination for an
    /// unlisted wing would fall through to `default_mode` (mirrors
    /// `coordination_federation_enabled`'s reasoning — see its doc comment).
    ///
    /// The ID-referencing fallbacks below have no wing to resolve a *specific* coordination
    /// route against (that is their entire reason for existing — see the section comment
    /// above), so this cannot narrow to "the one remote this call should use" the way a
    /// wing-based route resolution would. What it *can* do is exclude remotes that were never
    /// configured for coordination at all — e.g. a remote wired up only for `federation.wings`
    /// drawer routing — so a task/message/artifact/result lookup or write no longer probes a
    /// palace that has nothing to do with coordination. Deviation 9 in
    /// `docs/Coordination-Phase-3-Design.md` records why this candidate set exists.
    fn coordination_candidate_remotes(&self) -> std::collections::BTreeSet<&str> {
        let mut candidates = std::collections::BTreeSet::new();
        for rule in self.rules.coordination.values() {
            if let Some(name) = &rule.remote {
                candidates.insert(name.as_str());
            }
        }
        if self.rules.default_mode != RouteMode::Local {
            if let Some(name) = &self.rules.default_remote {
                candidates.insert(name.as_str());
            }
        }
        candidates
    }

    /// The single source of truth for "which configured remotes may actually be asked a
    /// coordination question" — `self.remotes` narrowed to `coordination_candidate_remotes()`,
    /// in name order (`self.remotes` is a `BTreeMap`).
    ///
    /// This exists because the same narrowing was previously hand-copied at five separate call
    /// sites — `coordination_read_fallback`, `coordination_write_fallback`,
    /// `coordination_task_revisioned_fallback`, `coordination_events_fanout`, and
    /// `coordination_inbox_fanout` — and the fan-out pair drifted out of sync with the other
    /// three twice in a row: `bd7cd21` added the coordination opt-in/diary gate to the fan-outs
    /// after it was fixed on the fallbacks first, and `e3fa83b` then added the candidate-set
    /// narrowing to the three fallbacks while leaving both fan-outs iterating `&self.remotes`
    /// unfiltered. A single helper cannot stop a future call site from re-introducing the bug in
    /// a *new* sixth loop, but it does mean the five that exist today share one implementation
    /// rather than five manually-synchronised copies, so a fix (or a future change to the
    /// candidate-set rule itself) here reaches all five at once instead of requiring someone to
    /// remember to update each one by hand.
    fn coordination_candidates(&self) -> impl Iterator<Item = (&String, &Arc<dyn RemoteApi>)> {
        let candidates = self.coordination_candidate_remotes();
        self.remotes.iter().filter(move |(name, _)| candidates.contains(name.as_str()))
    }

    /// Try a coordination *read* against each candidate remote (see
    /// `coordination_candidate_remotes`) in name order, returning the first success annotated
    /// with `origin` (the `remote:`-prefixed form, matching `changes_fanout`/`kg_add_remote`
    /// rather than the bare form `check_duplicate_all_remotes` uses for search results —
    /// coordination conflict/response payloads already read more like the changes feed's
    /// `origin` shape than a search hit).
    ///
    /// Error policy (shares the `CapabilityMissing` handling with `coordination_write_fallback`
    /// — see that method's doc comment — but is not otherwise symmetric with it): a `404` or a
    /// `CapabilityMissing` both mean "not this palace, try the next candidate". A `404` is the
    /// remote definitively saying it does not have this particular record; `CapabilityMissing`
    /// is the remote definitively saying it does not implement coordination at all — decided
    /// live from the remote's `/v1/info` capability list, independent of whether
    /// `federation.coordination` names it as a candidate. Both are a *positive, structural*
    /// answer of absence, not an ambiguous one, so treating them alike is correct for a read: it
    /// is the same "not this palace" case whether the remote says "no such task" or "I don't
    /// speak coordination at all". An `Unreachable` remote is the one genuinely degradable
    /// transport failure and is skipped the same way, because a down remote must not block
    /// discovery through the others (the federation-wide "reads degrade" rule — see
    /// `docs/Federation.md`). Every other error — `Unauthorized`, `VersionSkew`,
    /// `InvalidResponse`, `InvalidConfig`, or a non-404 `RemoteRejected` — is left terminal
    /// because it cannot be read as a definitive answer: `Unauthorized` means the token is
    /// wrong, so the record may well exist and reporting absence would be a lie; `VersionSkew`
    /// means the remote's protocol version cannot be trusted to answer at all. Those are surfaced
    /// as a hard `ToolError` immediately: silently treating them as a miss would let a caller
    /// believe a record does not exist when the truth is "your token is wrong" or "this remote
    /// is on an incompatible version". `Ok(None)` means every candidate was tried and genuinely
    /// does not have it.
    ///
    /// **Iteration is sequential, not concurrent like the fan-outs
    /// (`coordination_events_fanout`/`coordination_inbox_fanout`), and this is deliberate, not an
    /// oversight.** The fan-outs are aggregate reads: every candidate's answer is wanted and
    /// reported, so there is no "first hit wins" to lose by asking them all at once. This method
    /// is a discovery lookup for a single record: probing stops at the first success, so the
    /// candidates after the winner are never even asked. Making that concurrent would not change
    /// what is returned, only what is sent — every candidate would receive the id being looked up
    /// on every local miss, unconditionally, whether or not it turns out to hold the record.
    /// Per `CLAUDE.md`'s "memory never leaves the user's control by default", broadcasting a
    /// caller's query to remotes that do not have the answer is a real data-minimisation
    /// regression, traded only for latency on a path that already runs after a local miss —
    /// exactly where the local-first, one-remote-at-a-time contract is supposed to keep the
    /// common case from touching the network at all. Sequential order is therefore load-bearing,
    /// not incidental; do not "optimise" it to concurrent without re-litigating this trade-off.
    async fn coordination_read_fallback<F, Fut, T>(&self, op: F) -> ToolResult<Option<Value>>
    where
        F: Fn(Arc<dyn RemoteApi>) -> Fut,
        Fut: std::future::Future<Output = mempalace_remote::Result<T>>,
        T: serde::Serialize,
    {
        if !self.coordination_federation_enabled() {
            return Ok(None);
        }
        for (name, api) in self.coordination_candidates() {
            match op(Arc::clone(api)).await {
                Ok(dto) => {
                    let mut value = serde_json::to_value(&dto).unwrap_or_else(|_| json!({}));
                    if let Some(obj) = value.as_object_mut() {
                        obj.insert("origin".to_owned(), json!(format_remote_origin(name)));
                    }
                    return Ok(Some(value));
                }
                Err(RemoteError::RemoteRejected { status: 404, .. }) => continue,
                Err(RemoteError::CapabilityMissing { .. }) => continue,
                Err(e) if e.is_degradable() => {
                    tracing::warn!(
                        remote = %name,
                        error = %e,
                        "coordination read fallback: remote unreachable, trying next"
                    );
                }
                Err(e) => {
                    return Err(ToolError::Internal(McpError::Federation(format!(
                        "remote `{name}` coordination read failed: {e}"
                    ))));
                }
            }
        }
        Ok(None)
    }

    /// Try a coordination *write that references an existing record* against each candidate
    /// remote (see `coordination_candidate_remotes`) in name order.
    ///
    /// Error policy (shares the `404`/`CapabilityMissing` handling with
    /// `coordination_read_fallback` but is not otherwise symmetric with it — see that method's
    /// doc comment): a `404` or `CapabilityMissing` both mean "not this palace, try the
    /// next candidate" — `CapabilityMissing` is decided live from the remote's `/v1/info`
    /// capability list, independent of whether `federation.coordination` names it as a
    /// candidate, so a candidate remote can still turn out to be running a pre-Stage-4 server
    /// with no coordination support at all; that is exactly the "not this palace" case, not a
    /// misconfiguration to report. Every other error — including an unreachable remote — stops
    /// the search and surfaces as a hard `ToolError`: unlike a read, a write cannot afford to
    /// guess past a remote it could not actually confirm an answer from, because moving on could
    /// create a second, divergent record for the same task on the wrong palace. `Ok(None)` means
    /// every candidate was tried and genuinely does not have the referenced record.
    async fn coordination_write_fallback<F, Fut, T>(&self, op: F) -> ToolResult<Option<Value>>
    where
        F: Fn(Arc<dyn RemoteApi>) -> Fut,
        Fut: std::future::Future<Output = mempalace_remote::Result<T>>,
        T: serde::Serialize,
    {
        if !self.coordination_federation_enabled() {
            return Ok(None);
        }
        for (name, api) in self.coordination_candidates() {
            match op(Arc::clone(api)).await {
                Ok(dto) => {
                    let mut value = serde_json::to_value(&dto).unwrap_or_else(|_| json!({}));
                    if let Some(obj) = value.as_object_mut() {
                        obj.insert("applied_to".to_owned(), json!(format_remote_origin(name)));
                    }
                    return Ok(Some(value));
                }
                Err(RemoteError::RemoteRejected { status: 404, .. }) => continue,
                Err(RemoteError::CapabilityMissing { .. }) => continue,
                Err(e) => {
                    return Err(ToolError::Internal(McpError::Federation(format!(
                        "remote `{name}` rejected the write: {e}"
                    ))));
                }
            }
        }
        Ok(None)
    }

    /// Resolve the coordination route for a task's wing (`task_create` only — see the section
    /// comment above). Thin wrapper so callers do not need to import
    /// `mempalace_config::resolve_coordination_route` directly.
    pub fn resolve_coordination_route(&self, wing: &str) -> ResolvedRouteRule {
        resolve_coordination_route(&self.rules, wing)
    }

    /// Create a task on the remote a `resolve_coordination_route` + `resolve_write_target`
    /// resolution selected. Mirrors `kg_add_remote`: the caller has already established the
    /// write target is `Remote`, so `remote_name` is always `route.remote`.
    pub async fn coordination_task_create_remote(
        &self,
        remote_name: &str,
        req: NewTaskRequest,
    ) -> ToolResult<Value> {
        let Some(api) = self.remotes.get(remote_name) else {
            return Err(ToolError::Internal(McpError::Federation(format!(
                "remote `{remote_name}` is not configured"
            ))));
        };
        match api.coordination_task_create(req).await {
            Ok(dto) => {
                let mut value = serde_json::to_value(&dto).unwrap_or_else(|_| json!({}));
                if let Some(obj) = value.as_object_mut() {
                    obj.insert("applied_to".to_owned(), json!(format_remote_origin(remote_name)));
                }
                Ok(value)
            }
            Err(e) => Err(ToolError::Internal(McpError::Federation(format!(
                "remote `{remote_name}` task_create failed: {e}"
            )))),
        }
    }

    /// Fall back to a task GET across remotes after a local miss.
    pub async fn coordination_task_get_fallback(&self, task_id: &str) -> ToolResult<Option<Value>> {
        let task_id = task_id.to_owned();
        self.coordination_read_fallback(move |api| {
            let task_id = task_id.clone();
            async move { api.coordination_task_get(&task_id).await }
        })
        .await
    }

    /// Fall back to a task claim across remotes after a local miss. `MemPalace never retries a
    /// conflicting write on the caller's behalf` (see `docs/Federation.md`): a revision
    /// conflict from the owning remote is surfaced verbatim via
    /// `revision_conflict_payload`, in the exact shape a local conflict already uses, not
    /// retried and not treated as "try the next remote" — a 409 means this *is* the owning
    /// remote, just currently contested.
    pub async fn coordination_task_claim_fallback(
        &self,
        task_id: &str,
        req: TaskLeaseRequest,
    ) -> ToolResult<Option<Value>> {
        let expected_revision = req.expected_revision;
        self.coordination_task_revisioned_fallback(
            task_id,
            req,
            expected_revision,
            |api, id, req| async move { api.coordination_task_claim(&id, req).await },
        )
        .await
    }

    /// Fall back to a lease renewal across remotes after a local miss. See
    /// [`Self::coordination_task_claim_fallback`] for the conflict-shape note.
    pub async fn coordination_task_renew_fallback(
        &self,
        task_id: &str,
        req: TaskLeaseRequest,
    ) -> ToolResult<Option<Value>> {
        let expected_revision = req.expected_revision;
        self.coordination_task_revisioned_fallback(
            task_id,
            req,
            expected_revision,
            |api, id, req| async move { api.coordination_task_renew(&id, req).await },
        )
        .await
    }

    /// Fall back to a state transition across remotes after a local miss. See
    /// [`Self::coordination_task_claim_fallback`] for the conflict-shape note. Thin wrapper over
    /// [`Self::coordination_task_revisioned_fallback`] — identical control flow to claim/renew,
    /// generic over the request type so `TransitionTaskRequest` shares the body with
    /// `TaskLeaseRequest`.
    pub async fn coordination_task_transition_fallback(
        &self,
        task_id: &str,
        req: TransitionTaskRequest,
    ) -> ToolResult<Option<Value>> {
        let expected_revision = req.expected_revision;
        self.coordination_task_revisioned_fallback(
            task_id,
            req,
            expected_revision,
            |api, id, req| async move { api.coordination_task_transition(&id, req).await },
        )
        .await
    }

    /// Shared body for [`Self::coordination_task_claim_fallback`],
    /// [`Self::coordination_task_renew_fallback`], and
    /// [`Self::coordination_task_transition_fallback`] — identical control flow across three
    /// `RemoteApi` methods that each return `RemoteRevisionedWrite<CoordinationTaskDto>`;
    /// generic over the request type (`TaskLeaseRequest` for claim/renew, `TransitionTaskRequest`
    /// for transition) so all three share one body. `expected_revision` is threaded through
    /// separately rather than pulled generically off `req` so this stays free of a bespoke
    /// accessor trait for two request shapes.
    ///
    /// This is a coordination *write that references an existing record* — see
    /// `coordination_write_fallback`'s doc comment for the error policy (only the candidate set
    /// from `coordination_candidate_remotes` is tried; `404`/`CapabilityMissing` mean "not this
    /// palace"; anything else is terminal). The one addition here is the revision-conflict arm:
    /// a `409`-shaped conflict from a candidate means this *is* the record's owning remote, just
    /// currently contested, so it is surfaced verbatim via `revision_conflict_payload` — the
    /// exact shape a local conflict already uses — rather than treated as "try the next
    /// candidate" or as a hard error. MemPalace never retries a conflicting write on the
    /// caller's behalf (see `docs/Federation.md`); the envelope nests the task DTO under `task`
    /// to match the local success shape (`{"success": true, "task": {...}}`) — see
    /// `docs/Coordination-Phase-3-Design.md` deviation 9.
    async fn coordination_task_revisioned_fallback<F, Fut, Req>(
        &self,
        task_id: &str,
        req: Req,
        expected_revision: i64,
        call: F,
    ) -> ToolResult<Option<Value>>
    where
        F: Fn(Arc<dyn RemoteApi>, String, Req) -> Fut,
        Fut: std::future::Future<
                Output = mempalace_remote::Result<
                    RemoteRevisionedWrite<mempalace_federation::CoordinationTaskDto>,
                >,
            >,
        Req: Clone,
    {
        if !self.coordination_federation_enabled() {
            return Ok(None);
        }
        for (name, api) in self.coordination_candidates() {
            match call(Arc::clone(api), task_id.to_owned(), req.clone()).await {
                Ok(RemoteRevisionedWrite::Applied(dto)) => {
                    let task_value = serde_json::to_value(&dto).unwrap_or_else(|_| json!({}));
                    return Ok(Some(json!({
                        "success": true,
                        "task": task_value,
                        "applied_to": format_remote_origin(name),
                    })));
                }
                Ok(RemoteRevisionedWrite::Conflict { actual_revision }) => {
                    return Ok(Some(crate::revision_conflict_payload(
                        expected_revision,
                        actual_revision,
                    )));
                }
                Err(RemoteError::RemoteRejected { status: 404, .. }) => continue,
                Err(RemoteError::CapabilityMissing { .. }) => continue,
                Err(e) => {
                    return Err(ToolError::Internal(McpError::Federation(format!(
                        "remote `{name}` rejected the write: {e}"
                    ))));
                }
            }
        }
        Ok(None)
    }

    /// Fall back to sending a message across remotes after the referenced task_id is not found
    /// locally.
    pub async fn coordination_message_send_fallback(
        &self,
        req: NewMessageRequest,
    ) -> ToolResult<Option<Value>> {
        self.coordination_write_fallback(move |api| {
            let req = req.clone();
            async move { api.coordination_message_send(req).await }
        })
        .await
    }

    /// Fall back to a message GET across remotes after a local miss.
    pub async fn coordination_message_get_fallback(
        &self,
        message_id: &str,
    ) -> ToolResult<Option<Value>> {
        let message_id = message_id.to_owned();
        self.coordination_read_fallback(move |api| {
            let message_id = message_id.clone();
            async move { api.coordination_message_get(&message_id).await }
        })
        .await
    }

    /// Fall back to acknowledging a message across remotes after a local miss.
    pub async fn coordination_message_ack_fallback(
        &self,
        message_id: &str,
        req: AckMessageRequest,
    ) -> ToolResult<Option<Value>> {
        self.coordination_write_fallback(move |api| {
            let message_id = message_id.to_owned();
            let req = req.clone();
            async move { api.coordination_message_ack(&message_id, req).await }
        })
        .await
    }

    /// Fall back to storing an artifact across remotes after the referenced task_id is not
    /// found locally.
    pub async fn coordination_artifact_put_fallback(
        &self,
        req: NewArtifactRequest,
    ) -> ToolResult<Option<Value>> {
        self.coordination_write_fallback(move |api| {
            let req = req.clone();
            async move { api.coordination_artifact_put(req).await }
        })
        .await
    }

    /// Fall back to an artifact GET across remotes after a local miss.
    pub async fn coordination_artifact_get_fallback(
        &self,
        artifact_id: &str,
    ) -> ToolResult<Option<Value>> {
        let artifact_id = artifact_id.to_owned();
        self.coordination_read_fallback(move |api| {
            let artifact_id = artifact_id.clone();
            async move { api.coordination_artifact_get(&artifact_id).await }
        })
        .await
    }

    /// Fall back to storing a result across remotes after the referenced task_id is not found
    /// locally.
    pub async fn coordination_result_put_fallback(
        &self,
        req: NewTaskResultRequest,
    ) -> ToolResult<Option<Value>> {
        self.coordination_write_fallback(move |api| {
            let req = req.clone();
            async move { api.coordination_result_put(req).await }
        })
        .await
    }

    /// Fall back to a result GET across remotes after a local miss.
    pub async fn coordination_result_get_fallback(
        &self,
        result_id: &str,
    ) -> ToolResult<Option<Value>> {
        let result_id = result_id.to_owned();
        self.coordination_read_fallback(move |api| {
            let result_id = result_id.clone();
            async move { api.coordination_result_get(&result_id).await }
        })
        .await
    }

    /// Fan out a coordination-events query, concurrently and with a per-remote opaque cursor, to
    /// every remote in [`Self::coordination_candidates`] — not `self.remotes` — the
    /// coordination-feed counterpart of `changes_fanout`, reusing its `{unreachable, error}`
    /// isolation contract for genuine failures: one down remote never poisons a healthy one. A
    /// remote configured only for drawer or KG federation, never named by any
    /// `federation.coordination` rule, is skipped entirely rather than probed — the same
    /// candidate narrowing the ID-discovery fallbacks above apply, so this aggregate feed cannot
    /// send a recipient/wing/task_id filter to a palace that has nothing to do with coordination.
    /// Returns a map of `remote_name → per-remote result`; on success the value is
    /// `{ "events": [...], "next_cursor": <string|null> }` with each event annotated `"origin":
    /// "remote:<name>"`. A candidate that answers `CapabilityMissing` — it never actually runs
    /// coordination, decided live from its own `/v1/info` — is reported as `{"capability_missing":
    /// true, "capability": "...", "error": "..."}`, distinguishable from a genuinely unreachable
    /// remote's `{"unreachable": true, "error": "..."}`; see [`CoordinationFanoutFailure`].
    ///
    /// Gated the same way the ID-discovery fallbacks above are (see
    /// `coordination_federation_enabled`'s doc comment): this method, not just its callers, must
    /// refuse to contact any remote when coordination federation was never configured, and must
    /// refuse to send a `wing_agents`-shaped query to any remote regardless of configuration (see
    /// [`wing_blocks_coordination_fanout`]). The gate lives here rather than at each call site so
    /// a future aggregate-read call site cannot forget it — the same reasoning `c1166d7` used for
    /// pushing the diary/normalisation check into `resolve_coordination_route` itself.
    pub async fn coordination_events_fanout(
        &self,
        task_id: Option<String>,
        wing: Option<String>,
        limit: Option<usize>,
        cursors: &BTreeMap<String, String>,
    ) -> BTreeMap<String, Value> {
        if !self.coordination_federation_enabled()
            || wing_blocks_coordination_fanout(wing.as_deref())
        {
            return BTreeMap::new();
        }
        let mut set: JoinSet<(String, Result<Value, CoordinationFanoutFailure>)> = JoinSet::new();
        for (name, api) in self.coordination_candidates() {
            let name = name.clone();
            let api = Arc::clone(api);
            let query = CoordinationEventsQuery {
                cursor: cursors.get(&name).cloned(),
                task_id: task_id.clone(),
                wing: wing.clone(),
                limit,
            };
            set.spawn(async move {
                match api.coordination_events(query).await {
                    Ok(resp) => {
                        let origin = format_remote_origin(&name);
                        let events: Vec<Value> = resp
                            .events
                            .into_iter()
                            .map(|evt| {
                                let mut v =
                                    serde_json::to_value(&evt).unwrap_or_else(|_| json!({}));
                                if let Some(obj) = v.as_object_mut() {
                                    obj.insert("origin".to_owned(), json!(origin));
                                }
                                v
                            })
                            .collect();
                        (name, Ok(json!({ "events": events, "next_cursor": resp.next_cursor })))
                    }
                    Err(e) => (name, Err(CoordinationFanoutFailure::from_remote_error(e))),
                }
            });
        }

        let mut results: BTreeMap<String, Value> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(payload))) => {
                    results.insert(name, payload);
                }
                Ok((name, Err(failure))) => {
                    tracing::warn!(
                        remote = %name,
                        "coordination events fan-out failed: {}",
                        failure.message()
                    );
                    results.insert(name, failure.into_json());
                }
                Err(join_err) => {
                    tracing::warn!("coordination events fan-out task panicked: {join_err}");
                }
            }
        }
        results
    }

    /// Fan out an inbox read, concurrently and with a per-remote opaque cursor, to every remote
    /// in [`Self::coordination_candidates`] — not `self.remotes`. Same candidate narrowing,
    /// isolation contract, and `CapabilityMissing` vs. genuinely-unreachable distinction as
    /// [`Self::coordination_events_fanout`]/`changes_fanout` — see that method's doc comment.
    /// Returns a map of `remote_name → per-remote result`; on success the value is
    /// `{ "messages": [...], "next_cursor": <string|null> }` with each message annotated
    /// `"origin": "remote:<name>"`.
    pub async fn coordination_inbox_fanout(
        &self,
        recipient: String,
        wing: Option<String>,
        limit: Option<usize>,
        unacknowledged_only: bool,
        cursors: &BTreeMap<String, String>,
    ) -> BTreeMap<String, Value> {
        if !self.coordination_federation_enabled()
            || wing_blocks_coordination_fanout(wing.as_deref())
        {
            return BTreeMap::new();
        }
        let mut set: JoinSet<(String, Result<Value, CoordinationFanoutFailure>)> = JoinSet::new();
        for (name, api) in self.coordination_candidates() {
            let name = name.clone();
            let api = Arc::clone(api);
            let query = InboxQuery {
                recipient: recipient.clone(),
                wing: wing.clone(),
                cursor: cursors.get(&name).cloned(),
                limit,
                unacknowledged_only,
            };
            set.spawn(async move {
                match api.coordination_inbox(query).await {
                    Ok(resp) => {
                        let origin = format_remote_origin(&name);
                        let messages: Vec<Value> = resp
                            .messages
                            .into_iter()
                            .map(|msg| {
                                let mut v =
                                    serde_json::to_value(&msg).unwrap_or_else(|_| json!({}));
                                if let Some(obj) = v.as_object_mut() {
                                    obj.insert("origin".to_owned(), json!(origin));
                                }
                                v
                            })
                            .collect();
                        (name, Ok(json!({ "messages": messages, "next_cursor": resp.next_cursor })))
                    }
                    Err(e) => (name, Err(CoordinationFanoutFailure::from_remote_error(e))),
                }
            });
        }

        let mut results: BTreeMap<String, Value> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(payload))) => {
                    results.insert(name, payload);
                }
                Ok((name, Err(failure))) => {
                    tracing::warn!(
                        remote = %name,
                        "coordination inbox fan-out failed: {}",
                        failure.message()
                    );
                    results.insert(name, failure.into_json());
                }
                Err(join_err) => {
                    tracing::warn!("coordination inbox fan-out task panicked: {join_err}");
                }
            }
        }
        results
    }
}

/// Classification of a per-remote failure in a coordination aggregate fan-out
/// (`coordination_events_fanout`/`coordination_inbox_fanout`), so the two are reported with
/// distinguishable shapes in the result map instead of both collapsing into `"unreachable":
/// true`. A candidate remote that declines with `RemoteError::CapabilityMissing` — decided live
/// from its own `/v1/info` capability list, the same way the ID-discovery write fallbacks
/// already treat it as "not this palace" rather than a failure — correctly answered "I do not do
/// coordination"; it is not down, and reporting it as `unreachable` would send an operator
/// looking for an outage that never happened. Everything else (genuinely unreachable,
/// unauthorized, version-skewed, malformed response, …) keeps the original `unreachable` shape.
enum CoordinationFanoutFailure {
    /// The remote does not advertise the `coordination` capability at all.
    CapabilityMissing { capability: String, message: String },
    /// Every other per-remote error.
    Unreachable(String),
}

impl CoordinationFanoutFailure {
    fn from_remote_error(e: RemoteError) -> Self {
        match &e {
            RemoteError::CapabilityMissing { capability, .. } => {
                Self::CapabilityMissing { capability: capability.clone(), message: e.to_string() }
            }
            _ => Self::Unreachable(e.to_string()),
        }
    }

    /// The underlying error text, for logging regardless of which variant this is.
    fn message(&self) -> &str {
        match self {
            Self::CapabilityMissing { message, .. } | Self::Unreachable(message) => message,
        }
    }

    fn into_json(self) -> Value {
        match self {
            Self::CapabilityMissing { capability, message } => {
                json!({ "capability_missing": true, "capability": capability, "error": message })
            }
            Self::Unreachable(message) => json!({ "unreachable": true, "error": message }),
        }
    }
}

/// Whether a requested wing must suppress remote coordination fan-out.
///
/// `None` (no wing filter) never blocks — an unfiltered aggregate read has no wing to protect
/// and is unchanged by this fix. A `Some(wing)` blocks when it normalises to
/// [`SHARED_AGENT_DIARY_WING`] under [`WingId::normalized`] — trimmed, lowercased and prefixed,
/// so `"agents"`, `"Wing_Agents"` and `" wing_agents "` all match even though none of them `==`
/// the constant verbatim. This is exactly the bypass `c1166d7` closed on `tool_task_create`; a
/// verbatim `==` here would silently reopen it on the aggregate-read routes. A wing that fails to
/// normalise at all (e.g. empty after stripping) fails CLOSED — block rather than fan out —
/// mirroring `resolve_coordination_route`'s fail-closed direction on the same condition.
fn wing_blocks_coordination_fanout(wing: Option<&str>) -> bool {
    match wing {
        None => false,
        Some(raw) => match WingId::normalized(raw) {
            Ok(canonical) => canonical.as_str() == SHARED_AGENT_DIARY_WING,
            Err(_) => true,
        },
    }
}

// ─── KG helpers ─────────────────────────────────────────────────────────────

/// Annotate every fact in the `facts` or `timeline` array with `"origin"`.
fn annotate_kg_facts_origin(payload: &mut Value, origin: &str) {
    for key in &["facts", "timeline"] {
        if let Some(items) = payload.get_mut(*key).and_then(|v| v.as_array_mut()) {
            for item in items {
                if let Some(obj) = item.as_object_mut() {
                    obj.insert("origin".to_owned(), json!(origin));
                }
            }
        }
    }
}

/// Merge two KG query payloads: dedupe `facts` on (subject, predicate, object,
/// valid_from, direction), annotate origin, recalculate count.
fn merge_kg_facts(local: Value, mut remote: Value, remote_name: &str) -> Value {
    annotate_kg_facts_origin(&mut remote, remote_name);

    let local_facts = local["facts"].as_array().cloned().unwrap_or_default();
    let remote_facts = remote["facts"].as_array().cloned().unwrap_or_default();

    let mut seen = std::collections::HashSet::new();
    let mut merged_facts = Vec::new();

    // Local first (preferred on collision), then remote.
    for fact in local_facts.into_iter().chain(remote_facts) {
        let key = kg_fact_dedup_key(&fact);
        if seen.insert(key) {
            merged_facts.push(fact);
        }
    }

    let count = merged_facts.len();
    let mut payload = local;
    payload["facts"] = json!(merged_facts);
    payload["count"] = json!(count);
    payload
}

/// Merge two KG timeline payloads: dedupe `timeline` entries on (subject,
/// predicate, object, valid_from), annotate origin, recalculate count.
fn merge_kg_timeline(local: Value, mut remote: Value, remote_name: &str) -> Value {
    annotate_kg_facts_origin(&mut remote, remote_name);

    let local_rows = local["timeline"].as_array().cloned().unwrap_or_default();
    let remote_rows = remote["timeline"].as_array().cloned().unwrap_or_default();

    let mut seen = std::collections::HashSet::new();
    let mut merged = Vec::new();

    for row in local_rows.into_iter().chain(remote_rows) {
        let key = kg_timeline_dedup_key(&row);
        if seen.insert(key) {
            merged.push(row);
        }
    }

    let count = merged.len();
    let mut payload = local;
    payload["timeline"] = json!(merged);
    payload["count"] = json!(count);
    payload
}

/// Format a remote name as an `origin` label, guarding against a doubled
/// `remote:` prefix in case a configured remote name already carries one.
fn format_remote_origin(name: &str) -> String {
    if name.starts_with("remote:") { name.to_owned() } else { format!("remote:{name}") }
}

/// Return whether a remote's HTTP 409 is the semantic duplicate response.  Federation errors
/// encode the server's machine-readable error code at the beginning of the body (`code: message`);
/// treating every 409 as a duplicate would hide operation-id conflicts and other authoritative
/// rejections from callers.
fn is_duplicate_rejection(error: &RemoteError) -> bool {
    let RemoteError::RemoteRejected { status: 409, body, .. } = error else {
        return false;
    };
    body.split_once(':').map(|(code, _)| code.trim()) == Some("duplicate")
}

static OPERATION_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Generate a unique, stable operation identifier for direct remote mutations when the caller
/// omitted one.
fn generate_operation_id() -> String {
    let now = time::OffsetDateTime::now_utc().unix_timestamp_nanos();
    let count = OPERATION_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut hasher = blake3::Hasher::new();
    hasher.update(&now.to_le_bytes());
    hasher.update(&count.to_le_bytes());
    hasher.update(&std::process::id().to_le_bytes());
    let hash = hasher.finalize();
    format!("op_{}", &hash.to_hex()[..32])
}

/// Build the caller-visible structured result for a direct remote mutation whose outcome
/// could not be confirmed ([`RemoteError::UnknownOutcome`]).
///
/// This is deliberately a distinguishable *outcome*, not a generic internal error and not an
/// authoritative failure: the request may have committed before the response was lost, so the
/// caller can retry with the same stable `operation_id` and the receiving receipt store will
/// authoritatively replay (or dedupe) it.
fn remote_mutation_unknown_outcome_value(error: &RemoteError, operation_id: &str) -> Value {
    let RemoteError::UnknownOutcome { remote, message } = error else {
        // Misuse guard: this helper is only reachable from `is_unknown_outcome()` arms.
        return json!({ "success": false, "outcome": "unknown_outcome" });
    };
    json!({
        "success": false,
        "outcome": "unknown_outcome",
        "remote": remote,
        "operation_id": operation_id,
        "error": message,
        "retry": "safe to retry with the same operation_id; the remote may already have applied the mutation",
    })
}

/// Machine-actionable classification of a [`RemoteError`] for structured read-degradation
/// warnings.
fn remote_error_classification(error: &RemoteError) -> &'static str {
    match error {
        RemoteError::Unreachable { .. } => "unreachable",
        RemoteError::Unauthorized { .. } => "unauthorized",
        RemoteError::VersionSkew { .. } => "version_skew",
        RemoteError::RemoteRejected { .. } => "rejected",
        RemoteError::InvalidResponse { .. } => "invalid_response",
        RemoteError::InvalidConfig { .. } => "invalid_config",
        RemoteError::CapabilityMissing { .. } => "capability_missing",
        RemoteError::UnknownOutcome { .. } => "unknown_outcome",
    }
}

/// Structured partial-read degradation record (issue #127 requires machine-actionable
/// degradation, not bare strings).
fn structured_degradation(remote: &str, kind: &str, error: &RemoteError) -> Value {
    json!({
        "code": "remote_read_degraded",
        "remote": remote,
        "kind": kind,
        "error": error.to_string(),
        "classification": remote_error_classification(error),
    })
}

/// Attach a structured degradation to a combined-read payload while preserving the legacy
/// string `warnings` field for backward compatibility. Both arrays append, so a payload that
/// already carries degradation from an earlier merge keeps every record.
fn degraded_read_payload(
    mut payload: Value,
    remote: &str,
    kind: &str,
    error: &RemoteError,
) -> Value {
    let warn_msg = format!("remote `{remote}` {kind} failed: {error}");
    match payload.get_mut("warnings").and_then(|v| v.as_array_mut()) {
        Some(arr) => arr.push(json!(warn_msg)),
        None => {
            payload["warnings"] = json!([warn_msg]);
        }
    }
    let degradation = structured_degradation(remote, kind, error);
    match payload.get_mut("degradations").and_then(|v| v.as_array_mut()) {
        Some(arr) => arr.push(degradation),
        None => {
            payload["degradations"] = json!([degradation]);
        }
    }
    payload
}

/// Merge two KG stats payloads: sum numeric fields, union relationship types.
fn merge_kg_stats(local: Value, remote: Value) -> Value {
    let mut merged = local.clone();
    if let Some(obj) = merged.as_object_mut() {
        for key in &["entities", "triples", "current_facts", "expired_facts"] {
            // Default missing keys to 0 on either side so a key present on only
            // one palace still contributes to the merged total.
            let local_val = obj.get(*key).and_then(|v| v.as_u64()).unwrap_or(0);
            let remote_val = remote.get(*key).and_then(|v| v.as_u64()).unwrap_or(0);
            obj.insert((*key).to_owned(), json!(local_val + remote_val));
        }
        // Union relationship_types
        let mut types_set = std::collections::BTreeSet::new();
        for source in [&local, &remote] {
            if let Some(types) = source["relationship_types"].as_array() {
                for t in types {
                    if let Some(s) = t.as_str() {
                        types_set.insert(s.to_owned());
                    }
                }
            }
        }
        obj["relationship_types"] = json!(types_set.into_iter().collect::<Vec<_>>());
    }
    merged
}

fn kg_fact_dedup_key(fact: &Value) -> String {
    format!(
        "{}|{}|{}|{}|{}",
        fact["subject"].as_str().unwrap_or(""),
        fact["predicate"].as_str().unwrap_or(""),
        fact["object"].as_str().unwrap_or(""),
        fact["valid_from"].as_str().unwrap_or(""),
        fact["direction"].as_str().unwrap_or(""),
    )
}

fn kg_timeline_dedup_key(row: &Value) -> String {
    format!(
        "{}|{}|{}|{}",
        row["subject"].as_str().unwrap_or(""),
        row["predicate"].as_str().unwrap_or(""),
        row["object"].as_str().unwrap_or(""),
        row["valid_from"].as_str().unwrap_or(""),
    )
}

// ─── Search helpers ─────────────────────────────────────────────────────────

fn drawer_result_to_value(result: RemoteDrawerResult, origin: &str) -> Value {
    let mut v = json!({
        "drawer_id": result.drawer_id,
        "wing": result.wing,
        "room": result.room,
        "similarity": crate::round_similarity(result.score),
        "text": result.content,
        "source_file": result.source_file,
        "origin": origin,
    });
    if let Some(c) = &result.content_hash {
        v["content_hash"] = json!(c);
    }
    if result.stale {
        v["stale"] = json!(true);
    }
    v
}

fn search_payload(
    query: &str,
    wing: Option<&str>,
    room: Option<&str>,
    results: Vec<Value>,
    warnings: &[String],
    degradations: &[Value],
) -> Value {
    let mut payload = json!({
        "query": query,
        "filters": {
            "wing": wing,
            "room": room,
        },
        "results": results,
    });
    if !warnings.is_empty() {
        payload["warnings"] = json!(warnings);
    }
    if !degradations.is_empty() {
        payload["degradations"] = json!(degradations);
    }
    payload
}

/// N-way rank interleave across origins. `origins` is a list of (origin_name,
/// results) pairs. Local (if present) is expected to be first in the list.
/// Deduplication prefers the first seen (local preferred since it's first).
/// Truncates to `limit`.
pub(crate) fn merge_search_results_nway(
    mut origins: Vec<(String, Vec<Value>)>,
    limit: usize,
) -> Vec<Value> {
    match origins.as_slice() {
        [] => return vec![],
        [_] => {
            // The slice pattern proves exactly one origin is present, so
            // `remove(0)` can never panic — no `.unwrap()` needed to express it.
            let (origin_name, results) = origins.remove(0);
            return results
                .into_iter()
                .take(limit)
                .map(|mut v| {
                    if v.get("origin").is_none() {
                        v["origin"] = json!(origin_name);
                    }
                    v
                })
                .collect();
        }
        _ => {}
    }

    let mut merged: Vec<Value> = Vec::with_capacity(limit);
    let mut seen_ids = std::collections::HashSet::new();
    let mut seen_hashes = std::collections::HashSet::new();
    let mut seen_texts = std::collections::HashSet::new();

    let max_rank = origins.iter().map(|(_, r)| r.len()).max().unwrap_or(0);
    'outer: for rank in 0..max_rank {
        for (origin_name, results) in &origins {
            if merged.len() >= limit {
                break 'outer;
            }
            if rank < results.len() {
                let item = &results[rank];
                if !is_duplicate_search_item(item, &mut seen_ids, &mut seen_hashes, &mut seen_texts)
                {
                    let mut annotated = item.clone();
                    if annotated.get("origin").is_none() {
                        annotated["origin"] = json!(origin_name);
                    }
                    merged.push(annotated);
                }
            }
        }
    }

    merged.truncate(limit);
    merged
}

fn is_duplicate_search_item(
    item: &Value,
    seen_ids: &mut std::collections::HashSet<String>,
    seen_hashes: &mut std::collections::HashSet<String>,
    seen_texts: &mut std::collections::HashSet<String>,
) -> bool {
    // Stable drawer identity is authoritative. An item carrying a non-empty
    // drawer_id is deduped only against other IDs — never against hash or text
    // similarity, so replicated copies of logically distinct drawers (same
    // content, different stable identity) stay visible. Hash/text remain
    // compatibility fallbacks for legacy peers that do not return an ID, while
    // an ID-bearing row still seeds those fallback keys so a later no-ID peer
    // with identical content can be collapsed against it.
    let drawer_id = item["drawer_id"].as_str().filter(|id| !id.is_empty());
    let hash = item["content_hash"].as_str().filter(|h| !h.is_empty());
    let text = item["text"].as_str();

    let duplicate = match drawer_id {
        Some(id) => seen_ids.contains(id),
        None => {
            let hash_dup = hash.map(|h| seen_hashes.contains(h)).unwrap_or(false);
            let text_dup = text.map(|t| seen_texts.contains(t)).unwrap_or(false);
            hash_dup || text_dup
        }
    };
    if duplicate {
        return true;
    }

    // Not a duplicate — register stable identity, hash and text for future items.
    if let Some(id) = drawer_id {
        seen_ids.insert(id.to_owned());
    }
    if let Some(h) = hash {
        seen_hashes.insert(h.to_owned());
    }
    if let Some(t) = text {
        seen_texts.insert(t.to_owned());
    }
    false
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use mempalace_config::ResolvedRemote;
    use mempalace_federation::{
        AddDrawerRequest, AddDrawerResponse, ChangesQuery, ChangesResponse, CheckDuplicateRequest,
        CheckDuplicateResponse, DrawerSearchRequest, DrawerSearchResponse, InfoResponse,
        KgAddFactRequest, KgInvalidateRequest, KgQueryRequest, ListDrawersQuery,
        ListDrawersResponse,
    };
    use mempalace_remote::RemoteError;

    // ─── merge_search_results_nway unit tests ────────────────────────────────

    fn local_origins(items: Vec<Value>) -> Vec<(String, Vec<Value>)> {
        vec![("local".to_owned(), items)]
    }

    fn two_origins(local: Vec<Value>, remote: Vec<Value>) -> Vec<(String, Vec<Value>)> {
        let mut v = vec![];
        if !local.is_empty() {
            v.push(("local".to_owned(), local));
        }
        if !remote.is_empty() {
            v.push(("alpha".to_owned(), remote));
        }
        v
    }

    #[test]
    fn merge_interleaves_and_dedupes() {
        let local = vec![
            json!({"wing":"w","room":"r1","similarity":0.9,"text":"hello local"}),
            json!({"wing":"w","room":"r2","similarity":0.7,"text":"world local"}),
        ];
        let remote = vec![
            json!({"wing":"w","room":"r1","similarity":0.85,"text":"hello remote"}),
            json!({"wing":"w","room":"r3","similarity":0.7,"text":"new remote"}),
        ];
        let merged = merge_search_results_nway(two_origins(local, remote), 10);
        // Rank interleave: L0, R0, L1, R1 — no deduping (all texts differ)
        assert_eq!(merged.len(), 4);
        assert_eq!(merged[0]["text"], "hello local");
        assert_eq!(merged[1]["text"], "hello remote");
        assert_eq!(merged[2]["text"], "world local");
        assert_eq!(merged[3]["text"], "new remote");
    }

    #[test]
    fn merge_dedupes_on_identical_text() {
        let local = vec![json!({"wing":"w","room":"r1","similarity":0.9,"text":"same content"})];
        let remote = vec![json!({"wing":"w","room":"r1","similarity":0.85,"text":"same content"})];
        let merged = merge_search_results_nway(two_origins(local, remote), 10);
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn merge_respects_limit() {
        let local = vec![
            json!({"wing":"w","room":"r1","similarity":0.9,"text":"a"}),
            json!({"wing":"w","room":"r2","similarity":0.8,"text":"b"}),
        ];
        let remote = vec![
            json!({"wing":"w","room":"r3","similarity":0.7,"text":"c"}),
            json!({"wing":"w","room":"r4","similarity":0.6,"text":"d"}),
        ];
        let merged = merge_search_results_nway(two_origins(local, remote), 2);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_truncates_longer_list_to_limit() {
        let local = vec![
            json!({"wing":"w","room":"r1","similarity":0.9,"text":"a"}),
            json!({"wing":"w","room":"r2","similarity":0.8,"text":"b"}),
            json!({"wing":"w","room":"r3","similarity":0.7,"text":"c"}),
        ];
        let merged = merge_search_results_nway(local_origins(local), 2);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_empty_remote_returns_local() {
        let local = vec![json!({"wing":"w","room":"r1","similarity":0.9,"text":"only local"})];
        let merged = merge_search_results_nway(local_origins(local), 5);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["text"], "only local");
        assert_eq!(merged[0]["origin"], "local");
    }

    #[test]
    fn merge_empty_local_returns_remote_truncated() {
        let remote = vec![
            json!({"wing":"w","room":"r1","similarity":0.9,"text":"a"}),
            json!({"wing":"w","room":"r2","similarity":0.8,"text":"b"}),
            json!({"wing":"w","room":"r3","similarity":0.7,"text":"c"}),
        ];
        let merged = merge_search_results_nway(vec![("alpha".to_owned(), remote)], 2);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn router_with_no_remotes_has_no_remotes() {
        let router = FederationRouter::new(FederationRuntimeConfig::default());
        assert!(!router.has_remotes());
    }

    #[test]
    fn merge_dedupes_on_content_hash() {
        let local = vec![
            json!({"wing":"w","room":"r1","similarity":0.9,"text":"hello","content_hash":"abc123"}),
        ];
        let remote = vec![
            json!({"wing":"w","room":"r2","similarity":0.8,"text":"hello","content_hash":"abc123"}),
        ];
        let merged = merge_search_results_nway(two_origins(local, remote), 10);
        // Local preferred on hash collision — remote skipped.
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["wing"], "w");
        assert_eq!(merged[0]["room"], "r1");
    }

    #[test]
    fn merge_dedupes_on_text_fallback() {
        let local = vec![json!({"wing":"w","room":"r1","text":"content"})];
        let remote = vec![json!({"wing":"w","room":"r2","text":"content"})];
        let merged = merge_search_results_nway(two_origins(local, remote), 10);
        // No content_hash, falls back to text dedupe.
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn merge_same_id_dedupes_even_with_different_content() {
        // Replicated copy of the same logical drawer: identical stable ID must be
        // one result even though the content differs between peers.
        let local = vec![json!({
            "wing":"w","room":"r1","text":"local copy","drawer_id":"d0001"
        })];
        let remote = vec![json!({
            "wing":"w","room":"r2","text":"remote copy","drawer_id":"d0001"
        })];
        let merged = merge_search_results_nway(two_origins(local, remote), 10);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["drawer_id"], "d0001");
    }

    #[test]
    fn merge_different_ids_with_same_content_keeps_both() {
        // Logically distinct drawers with identical content must remain two
        // results — dedupe is by stable identity, not semantic/content similarity.
        let local = vec![json!({
            "wing":"w","room":"r1","text":"same content","content_hash":"abc123","drawer_id":"d0001"
        })];
        let remote = vec![json!({
            "wing":"w","room":"r2","text":"same content","content_hash":"abc123","drawer_id":"d0002"
        })];
        let merged = merge_search_results_nway(two_origins(local, remote), 10);
        assert_eq!(merged.len(), 2);
        let ids: Vec<&str> = merged.iter().filter_map(|v| v["drawer_id"].as_str()).collect();
        assert_eq!(ids, vec!["d0001", "d0002"]);
    }

    #[test]
    fn merge_id_bearing_row_seeds_fallback_for_legacy_peer() {
        // A legacy no-ID peer with identical content is collapsed against an
        // earlier ID-bearing row via the seeded hash/text keys.
        let local = vec![json!({
            "wing":"w","room":"r1","text":"same content","content_hash":"abc123","drawer_id":"d0001"
        })];
        let remote = vec![json!({
            "wing":"w","room":"r2","text":"same content","content_hash":"abc123"
        })];
        let merged = merge_search_results_nway(two_origins(local, remote), 10);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["drawer_id"], "d0001");
    }

    #[test]
    fn merge_interleaves_deduped_and_non_deduped() {
        // Rank-0 remote item is a duplicate of rank-0 local → skipped.
        let local = vec![
            json!({"wing":"w","room":"r1","text":"alpha"}),
            json!({"wing":"w","room":"r2","text":"beta"}),
        ];
        let remote = vec![
            json!({"wing":"w","room":"r3","text":"alpha"}),
            json!({"wing":"w","room":"r4","text":"gamma"}),
        ];
        let merged = merge_search_results_nway(two_origins(local, remote), 10);
        // L0, R0(skipped), L1, R1
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0]["text"], "alpha");
        assert_eq!(merged[1]["text"], "beta");
        assert_eq!(merged[2]["text"], "gamma");
    }

    #[test]
    fn merge_three_way_interleave() {
        // 3-origin N-way merge
        let local = vec![json!({"text":"L0"}), json!({"text":"L1"})];
        let r1 = vec![json!({"text":"R1_0"}), json!({"text":"R1_1"})];
        let r2 = vec![json!({"text":"R2_0"}), json!({"text":"R2_1"})];
        let origins = vec![
            ("local".to_owned(), local),
            ("remote1".to_owned(), r1),
            ("remote2".to_owned(), r2),
        ];
        let merged = merge_search_results_nway(origins, 10);
        // rank 0: L0, R1_0, R2_0; rank 1: L1, R1_1, R2_1
        assert_eq!(merged.len(), 6);
        assert_eq!(merged[0]["text"], "L0");
        assert_eq!(merged[1]["text"], "R1_0");
        assert_eq!(merged[2]["text"], "R2_0");
        assert_eq!(merged[3]["text"], "L1");
        assert_eq!(merged[4]["text"], "R1_1");
        assert_eq!(merged[5]["text"], "R2_1");
    }

    #[test]
    fn merge_kg_facts_dedupes_and_counts() {
        let local = json!({
            "entity": "Alice",
            "facts": [
                {"subject":"A","predicate":"loves","object":"B","valid_from":"2026-01-01","direction":"outgoing"},
                {"subject":"C","predicate":"works_on","object":"A","valid_from":null,"direction":"incoming"},
            ],
            "count": 2
        });
        let remote = json!({
            "entity": "Alice",
            "facts": [
                {"subject":"A","predicate":"loves","object":"B","valid_from":"2026-01-01","direction":"outgoing"},
                {"subject":"A","predicate":"knows","object":"D","valid_from":"2026-02-01","direction":"outgoing"},
            ],
            "count": 2
        });
        let merged = merge_kg_facts(local, remote, "remote1");
        assert_eq!(merged["count"], 3);
        let facts = merged["facts"].as_array().unwrap();
        assert_eq!(facts.len(), 3);
        // First fact (local orig) has no origin annotation
        assert!(facts[0].get("origin").is_none());
        // Remote-only fact gets origin annotation
        let remote_fact = facts.iter().find(|f| f["origin"].as_str() == Some("remote1"));
        assert!(remote_fact.is_some());
    }

    #[test]
    fn merge_kg_timeline_dedupes() {
        let local = json!({
            "entity": "all",
            "timeline": [
                {"subject":"A","predicate":"loves","object":"B","valid_from":"2026-01-01","current":true},
            ],
            "count": 1
        });
        let remote = json!({
            "entity": "all",
            "timeline": [
                {"subject":"A","predicate":"loves","object":"B","valid_from":"2026-01-01","current":true},
                {"subject":"A","predicate":"knows","object":"C","valid_from":"2026-03-01","current":true},
            ],
            "count": 2
        });
        let merged = merge_kg_timeline(local, remote, "remote1");
        assert_eq!(merged["count"], 2);
    }

    #[test]
    fn merge_kg_stats_sums_numerics_and_unions_types() {
        let local = json!({
            "entities": 10,
            "triples": 25,
            "current_facts": 20,
            "expired_facts": 5,
            "relationship_types": ["loves", "works_on"],
        });
        let remote = json!({
            "entities": 7,
            "triples": 15,
            "current_facts": 10,
            "expired_facts": 5,
            "relationship_types": ["loves", "knows"],
        });
        let merged = merge_kg_stats(local, remote);
        assert_eq!(merged["entities"], 17);
        assert_eq!(merged["triples"], 40);
        assert_eq!(merged["current_facts"], 30);
        assert_eq!(merged["expired_facts"], 10);
        let types: Vec<String> = merged["relationship_types"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_owned()))
            .collect();
        assert_eq!(types, vec!["knows", "loves", "works_on"]);
    }

    #[test]
    fn wing_availability_annotates_local_and_default() {
        let rules = FederationRuntimeConfig::default();
        let router = FederationRouter::new(rules);
        let mut wings = BTreeMap::new();
        wings.insert("wing_code".to_owned(), 5);
        let avail = router.wing_availability(&wings);
        assert_eq!(avail["wing_code"], "local");
    }

    #[test]
    fn wing_availability_default_combined_gives_combined() {
        // When default_mode is Combined and no wing-specific rule, availability
        // should be "combined".
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        let rules = FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Combined,
            default_remote: Some("alpha".to_owned()),
            wings: BTreeMap::new(),
            kg: None,
            coordination: BTreeMap::new(),
        };
        let router_obj = FederationRouter::new(rules);
        let mut wings = BTreeMap::new();
        wings.insert("wing_code".to_owned(), 5);
        let avail = router_obj.wing_availability(&wings);
        assert_eq!(avail["wing_code"], "combined");
    }

    // ─── coordination_availability (issue #125) ──────────────────────────────

    #[test]
    fn coordination_availability_includes_coordination_only_wing() {
        // A wing named only in federation.coordination — no drawers, no
        // federation.wings entry — must still appear. This is the core defect:
        // wing_availability's key set never sees such a wing at all.
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_tasks".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        let rules = FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings: BTreeMap::new(),
            kg: None,
            coordination,
        };
        let router = FederationRouter::new(rules);

        // No drawers locally, and no federation.wings entry either.
        let local_wings: BTreeMap<String, usize> = BTreeMap::new();

        // The drawer map (unchanged behaviour) never sees this wing.
        let drawer_avail = router.wing_availability(&local_wings);
        assert!(
            drawer_avail.get("wing_tasks").is_none(),
            "wing_availability must not be changed by this fix"
        );

        // The new coordination map does.
        let coord_avail = router.coordination_availability(&local_wings);
        assert_eq!(coord_avail["wing_tasks"], "remote:alpha");
    }

    #[test]
    fn coordination_availability_diverges_from_drawer_availability() {
        // A wing whose drawer routing and coordination routing differ must
        // report both correctly and independently — the conflation defect.
        let mut wings = BTreeMap::new();
        wings.insert(
            "wing_x".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Combined,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Both,
            },
        );
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_x".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        let rules = FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings,
            kg: None,
            coordination,
        };
        let router = FederationRouter::new(rules);
        let local_wings: BTreeMap<String, usize> = BTreeMap::new();

        let drawer_avail = router.wing_availability(&local_wings);
        let coord_avail = router.coordination_availability(&local_wings);
        assert_eq!(drawer_avail["wing_x"], "combined");
        assert_eq!(coord_avail["wing_x"], "remote:alpha");
    }

    #[test]
    fn coordination_availability_wing_agents_always_local() {
        // wing_agents must report "local" unconditionally — even when
        // default_mode is Remote and even when a federation.coordination
        // rule tries to route it elsewhere. This must fall out of
        // resolve_coordination_route's hard override, not a special case here.
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_agents".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        let rules = FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Remote,
            default_remote: Some("alpha".to_owned()),
            wings: BTreeMap::new(),
            kg: None,
            coordination,
        };
        let router = FederationRouter::new(rules);
        let mut local_wings = BTreeMap::new();
        local_wings.insert("wing_agents".to_owned(), 3);

        let coord_avail = router.coordination_availability(&local_wings);
        assert_eq!(coord_avail["wing_agents"], "local");
    }

    #[test]
    fn coordination_availability_falls_through_to_default_mode() {
        // A wing with no rule anywhere (not in local drawers... it IS a local
        // wing here, but has no federation.wings or federation.coordination
        // entry) falls through to default_mode.
        //
        // This is the headline regression test (Codex P2, PR #126, comment 3873465832): the
        // real-world config that motivated this fix is exactly this shape — `default_mode:
        // combined`, no explicit `federation.coordination` entry for the wing.
        // `rule_from_default_mode` maps that to `mode: Combined, write: Local`, so the task is
        // always placed locally. Reporting the mode (the pre-fix behaviour) would print
        // `"combined"` here, which is wrong: it does not identify where a task lands, and on the
        // real palace that motivated this fix, 19 of 20 wings fell through this exact path and
        // every one of them placed tasks locally while reporting `"combined"`. This test
        // previously asserted `"combined"` — that assertion was pinning the defect, not
        // documented behaviour; it is corrected here to assert the effective write target,
        // `"local"`.
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        let rules = FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Combined,
            default_remote: Some("alpha".to_owned()),
            wings: BTreeMap::new(),
            kg: None,
            coordination: BTreeMap::new(),
        };
        let router = FederationRouter::new(rules);
        let mut local_wings = BTreeMap::new();
        local_wings.insert("wing_unlisted".to_owned(), 2);

        let coord_avail = router.coordination_availability(&local_wings);
        assert_eq!(coord_avail["wing_unlisted"], "local");
    }

    #[test]
    fn coordination_availability_explicit_combined_write_remote_reports_remote() {
        // An explicit `mode: combined` coordination rule with `write: remote` must report
        // "remote:<name>" — the effective write target, not the "combined" mode.
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_combined_remote".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Combined,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        let rules = FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings: BTreeMap::new(),
            kg: None,
            coordination,
        };
        let router = FederationRouter::new(rules);
        let local_wings: BTreeMap<String, usize> = BTreeMap::new();

        let coord_avail = router.coordination_availability(&local_wings);
        assert_eq!(coord_avail["wing_combined_remote"], "remote:alpha");
    }

    #[test]
    fn coordination_availability_explicit_combined_write_local_reports_local() {
        // An explicit `mode: combined` coordination rule with `write: local` must report
        // "local" — the effective write target, not the "combined" mode.
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_combined_local".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Combined,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Local,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        let rules = FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings: BTreeMap::new(),
            kg: None,
            coordination,
        };
        let router = FederationRouter::new(rules);
        let local_wings: BTreeMap<String, usize> = BTreeMap::new();

        let coord_avail = router.coordination_availability(&local_wings);
        assert_eq!(coord_avail["wing_combined_local"], "local");
    }

    #[test]
    fn coordination_availability_explicit_remote_mode_unchanged() {
        // `mode: remote` must still report "remote:<name>" (unchanged by this fix — Remote
        // mode maps directly to WriteTarget::Remote either way).
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_remote".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        let rules = FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings: BTreeMap::new(),
            kg: None,
            coordination,
        };
        let router = FederationRouter::new(rules);
        let local_wings: BTreeMap<String, usize> = BTreeMap::new();

        let coord_avail = router.coordination_availability(&local_wings);
        assert_eq!(coord_avail["wing_remote"], "remote:alpha");
    }

    #[test]
    fn coordination_availability_and_wing_availability_diverge_on_combined_default() {
        // Pin the deliberate divergence: for the same fixture (a wing falling through
        // `default_mode: combined`), wing_availability (drawer routing) must still report the
        // *mode* ("combined" — a real, supported drawer configuration), while
        // coordination_availability must report the effective *write target* ("local" — a task
        // for this wing is always created locally because `federation.coordination` cannot
        // resolve to `write: both`). wing_availability's behaviour must be completely unchanged
        // by this fix.
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        let rules = FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Combined,
            default_remote: Some("alpha".to_owned()),
            wings: BTreeMap::new(),
            kg: None,
            coordination: BTreeMap::new(),
        };
        let router = FederationRouter::new(rules);
        let mut local_wings = BTreeMap::new();
        local_wings.insert("wing_unlisted".to_owned(), 2);

        let drawer_avail = router.wing_availability(&local_wings);
        let coord_avail = router.coordination_availability(&local_wings);
        assert_eq!(drawer_avail["wing_unlisted"], "combined");
        assert_eq!(coord_avail["wing_unlisted"], "local");
    }

    // ─── E2E federation tests with mock remote ──────────────────────────────

    struct MockRemote {
        info_response: Value,
        search_results: Vec<Value>,
        add_drawer_success: bool,
        add_drawer_409: bool,
        add_drawer_409_body: String,
        duplicate_matches: Vec<Value>,
        taxonomy: Value,
        wings: Value,
        rooms: Value,
        kg_query_response: Value,
        kg_timeline_response: Value,
        kg_stats_response: Value,
        kg_add_response: Value,
        kg_invalidate_response: Value,
        delete_succeeds: bool,
        fail_on: Option<String>,
        /// Endpoint that returns [`RemoteError::UnknownOutcome`] instead of its normal result
        /// (e.g. `"add_drawer"`, `"delete"`, `"kg_add"`, `"kg_invalidate"`).
        fail_unknown_on: Option<String>,
        /// When set, the first `add_drawer` returns [`RemoteError::UnknownOutcome`] (simulating a
        /// mutation that committed remotely but whose response was lost) and every later call
        /// succeeds — the shape of a server receipt-store replay.
        add_drawer_commit_then_unknown: bool,
        /// Number of `check_duplicate` calls, so a test can assert the operation-aware preflight
        /// bypass never reached the remote.
        check_duplicate_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        /// `operation_id` received by every `add_drawer` call, in order.
        received_add_operation_ids: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
        /// `operation_id` received by every `delete_drawer_with_operation_id` call, in order.
        received_delete_operation_ids: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
        /// `operation_id` received by every `kg_add_fact` call, in order.
        received_kg_add_operation_ids: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
        /// `operation_id` received by every `kg_invalidate` call, in order.
        received_kg_invalidate_operation_ids: std::sync::Arc<std::sync::Mutex<Vec<Option<String>>>>,
        /// Number of `add_drawer` calls, so the commit-then-lost mock can fail only the first.
        add_drawer_attempts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        changes_events: Vec<mempalace_federation::ChangeEventDto>,
        changes_next_cursor: Option<String>,
        /// When set, the `changes` call records the incoming cursor here.
        received_cursor: std::sync::Arc<std::sync::Mutex<Option<String>>>,
        received_search_view: std::sync::Arc<std::sync::Mutex<Option<String>>>,
        /// Bumped by every `coordination_*` method below. Lets a test assert that a
        /// coordination fallback never touched the remote at all — a plain `None`/`Ok(None)`
        /// result is not sufficient on its own, since a genuine remote miss produces the exact
        /// same result after a real round trip.
        coordination_calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        /// Configures what `coordination_task_get` returns; `NotFound` (the previous hard-coded
        /// behaviour) unless overridden. Used by the read-fallback error-policy tests.
        coordination_task_get_outcome: MockCoordOutcome,
        /// Configures what `coordination_task_claim`/`coordination_task_renew`/
        /// `coordination_task_transition` return; `NotFound` unless overridden. All three share
        /// one outcome field because the tests below only ever need one mock remote to behave
        /// consistently across whichever write is exercised.
        coordination_task_write_outcome: MockCoordOutcome,
    }

    impl Default for MockRemote {
        fn default() -> Self {
            Self {
                info_response: json!({
                    "server_version": "1.0.0-test",
                    "federation_api_version": 1,
                    "embedding_profile": "balanced",
                    "capabilities": ["drawers", "kg"]
                }),
                search_results: vec![],
                add_drawer_success: true,
                add_drawer_409: false,
                add_drawer_409_body: "duplicate: near-duplicate content".to_owned(),
                duplicate_matches: vec![],
                taxonomy: json!({"taxonomy": {}}),
                wings: json!({"wings": {}}),
                rooms: json!({"rooms": {}}),
                kg_query_response: json!({"entity":"","facts":[],"count":0}),
                kg_timeline_response: json!({"entity":"all","timeline":[],"count":0}),
                kg_stats_response: json!({"entities":0,"triples":0,"current_facts":0,"expired_facts":0,"relationship_types":[]}),
                kg_add_response: json!({"success": true}),
                kg_invalidate_response: json!({"success": true}),
                delete_succeeds: true,
                fail_on: None,
                fail_unknown_on: None,
                add_drawer_commit_then_unknown: false,
                check_duplicate_calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                received_add_operation_ids: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                received_delete_operation_ids: std::sync::Arc::new(std::sync::Mutex::new(
                    Vec::new(),
                )),
                received_kg_add_operation_ids: std::sync::Arc::new(std::sync::Mutex::new(
                    Vec::new(),
                )),
                received_kg_invalidate_operation_ids: std::sync::Arc::new(std::sync::Mutex::new(
                    Vec::new(),
                )),
                add_drawer_attempts: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                changes_events: vec![],
                changes_next_cursor: None,
                received_cursor: std::sync::Arc::new(std::sync::Mutex::new(None)),
                received_search_view: std::sync::Arc::new(std::sync::Mutex::new(None)),
                coordination_calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                coordination_task_get_outcome: MockCoordOutcome::NotFound,
                coordination_task_write_outcome: MockCoordOutcome::NotFound,
            }
        }
    }

    /// Canned outcomes for the configurable coordination mock methods below — lets a test drive
    /// `coordination_read_fallback`/`coordination_write_fallback` through every error variant
    /// `RemoteError` defines without hand-building a `RemoteError` (not `Clone`) per call.
    #[derive(Clone)]
    enum MockCoordOutcome {
        /// The exact-ID lookup / referenced record was not found on this remote — the
        /// "not this palace, try the next one" case for both reads and writes.
        NotFound,
        /// The remote rejected our credentials — terminal for both reads and writes.
        Unauthorized,
        /// The remote speaks an incompatible federation API version — terminal for both.
        VersionSkew,
        /// The remote doesn't advertise the `coordination` capability — terminal for reads,
        /// but "not this palace, try the next one" for writes (finding 2b).
        CapabilityMissing,
        /// The remote could not be reached — degradable (skip) for reads, terminal for writes.
        Unreachable,
        /// The remote returned an undecodable 2xx body — terminal for both.
        InvalidResponse,
        /// The task write applies successfully, returning this DTO.
        Applied(Box<mempalace_federation::CoordinationTaskDto>),
        /// The task write hit a stale `expected_revision`.
        Conflict(Option<i64>),
    }

    impl MockCoordOutcome {
        fn into_task_get_result(
            self,
        ) -> mempalace_remote::Result<mempalace_federation::CoordinationTaskDto> {
            match self {
                Self::Applied(dto) => Ok(*dto),
                other => Err(other.into_error()),
            }
        }

        fn into_task_write_result(
            self,
        ) -> mempalace_remote::Result<
            RemoteRevisionedWrite<mempalace_federation::CoordinationTaskDto>,
        > {
            match self {
                Self::Applied(dto) => Ok(RemoteRevisionedWrite::Applied(*dto)),
                Self::Conflict(actual_revision) => {
                    Ok(RemoteRevisionedWrite::Conflict { actual_revision })
                }
                other => Err(other.into_error()),
            }
        }

        fn into_error(self) -> RemoteError {
            match self {
                Self::NotFound => RemoteError::RemoteRejected {
                    remote: "mock".to_owned(),
                    status: 404,
                    body: "not found".to_owned(),
                },
                Self::Unauthorized => RemoteError::Unauthorized { remote: "mock".to_owned() },
                Self::VersionSkew => {
                    RemoteError::VersionSkew { remote: "mock".to_owned(), ours: 1, theirs: 2 }
                }
                Self::CapabilityMissing => RemoteError::CapabilityMissing {
                    remote: "mock".to_owned(),
                    capability: "coordination".to_owned(),
                },
                Self::Unreachable => RemoteError::Unreachable {
                    remote: "mock".to_owned(),
                    message: "mock remote is down".to_owned(),
                },
                Self::InvalidResponse => RemoteError::InvalidResponse {
                    remote: "mock".to_owned(),
                    message: "undecodable body".to_owned(),
                },
                Self::Applied(_) | Self::Conflict(_) => {
                    unreachable!("into_error is only called for the non-success/non-conflict arms")
                }
            }
        }
    }

    /// A minimal but complete `CoordinationTaskDto` for tests that only care about the
    /// envelope shape, not the task's actual field values.
    fn make_task_dto(task_id: &str) -> mempalace_federation::CoordinationTaskDto {
        mempalace_federation::CoordinationTaskDto {
            task_id: task_id.to_owned(),
            title: "test task".to_owned(),
            description: "d".to_owned(),
            state: mempalace_federation::CoordinationTaskState::Running,
            revision: 1,
            created_by: "alice".to_owned(),
            wing: "wing_team".to_owned(),
            owner: Some("worker-1".to_owned()),
            parent_id: None,
            dependencies: vec![],
            budget: None,
            lease_expires_at: Some("2026-01-01T00:05:00Z".to_owned()),
            expires_at: None,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
        }
    }

    #[async_trait::async_trait]
    impl RemoteApi for MockRemote {
        async fn info(&self) -> mempalace_remote::Result<InfoResponse> {
            self.check_fail("info")?;
            Ok(serde_json::from_value(self.info_response.clone()).unwrap())
        }

        async fn search_drawers(
            &self,
            req: DrawerSearchRequest,
        ) -> mempalace_remote::Result<DrawerSearchResponse> {
            self.check_fail("search")?;
            *self.received_search_view.lock().unwrap() = req.view;
            let results = self
                .search_results
                .iter()
                .enumerate()
                .map(|(i, v)| RemoteDrawerResult {
                    drawer_id: format!("remote-{}", i),
                    wing: v["wing"].as_str().unwrap_or("").to_owned(),
                    room: v["room"].as_str().unwrap_or("").to_owned(),
                    rank: i + 1,
                    score: v["similarity"].as_f64().unwrap_or(0.0) as f32,
                    content: v["text"].as_str().unwrap_or("").to_owned(),
                    source_file: v["source_file"].as_str().map(|s| s.to_owned()),
                    content_hash: v["content_hash"].as_str().map(|s| s.to_owned()),
                    filed_at: None,
                    added_by: None,
                    stale: false,
                })
                .collect();
            Ok(DrawerSearchResponse { results })
        }

        async fn check_duplicate(
            &self,
            _req: CheckDuplicateRequest,
        ) -> mempalace_remote::Result<CheckDuplicateResponse> {
            self.check_duplicate_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.check_fail("check_duplicate")?;
            Ok(CheckDuplicateResponse {
                is_duplicate: !self.duplicate_matches.is_empty(),
                matches: json!(self.duplicate_matches.clone()),
            })
        }

        async fn add_drawer(
            &self,
            req: AddDrawerRequest,
        ) -> mempalace_remote::Result<AddDrawerResponse> {
            self.received_add_operation_ids.lock().unwrap().push(req.operation_id.clone());
            let attempt =
                self.add_drawer_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.check_fail("add_drawer")?;
            if self.add_drawer_commit_then_unknown && attempt == 0 {
                return Err(RemoteError::UnknownOutcome {
                    remote: "mock".to_owned(),
                    message: "committed but response lost".to_owned(),
                });
            }
            if self.add_drawer_409 {
                return Err(RemoteError::RemoteRejected {
                    remote: "mock".to_owned(),
                    status: 409,
                    body: self.add_drawer_409_body.clone(),
                });
            }
            Ok(AddDrawerResponse {
                success: self.add_drawer_success,
                drawer_id: Some("rem-drawer-1".to_owned()),
                wing: Some(req.wing),
                room: Some(req.room),
            })
        }

        async fn list_drawers(
            &self,
            _query: ListDrawersQuery,
        ) -> mempalace_remote::Result<ListDrawersResponse> {
            self.check_fail("list")?;
            Ok(ListDrawersResponse { drawers: json!([]), next_cursor: None })
        }

        async fn get_drawer(&self, _drawer_id: &str) -> mempalace_remote::Result<Value> {
            self.check_fail("get_drawer")?;
            Ok(json!({}))
        }

        async fn delete_drawer(&self, _drawer_id: &str) -> mempalace_remote::Result<()> {
            self.check_fail("delete")?;
            if self.delete_succeeds {
                Ok(())
            } else {
                Err(RemoteError::RemoteRejected {
                    remote: "mock".to_owned(),
                    status: 404,
                    body: "not found".to_owned(),
                })
            }
        }

        async fn delete_drawer_with_operation_id(
            &self,
            drawer_id: &str,
            operation_id: Option<&str>,
        ) -> mempalace_remote::Result<()> {
            self.received_delete_operation_ids
                .lock()
                .unwrap()
                .push(operation_id.map(ToOwned::to_owned));
            self.delete_drawer(drawer_id).await
        }

        async fn kg_query(&self, _req: KgQueryRequest) -> mempalace_remote::Result<Value> {
            self.check_fail("kg_query")?;
            Ok(self.kg_query_response.clone())
        }

        async fn kg_add_fact(&self, req: KgAddFactRequest) -> mempalace_remote::Result<Value> {
            self.received_kg_add_operation_ids.lock().unwrap().push(req.operation_id.clone());
            self.check_fail("kg_add")?;
            Ok(self.kg_add_response.clone())
        }

        async fn kg_invalidate(&self, req: KgInvalidateRequest) -> mempalace_remote::Result<Value> {
            self.received_kg_invalidate_operation_ids
                .lock()
                .unwrap()
                .push(req.operation_id.clone());
            self.check_fail("kg_invalidate")?;
            Ok(self.kg_invalidate_response.clone())
        }

        async fn kg_timeline(&self, _entity: Option<&str>) -> mempalace_remote::Result<Value> {
            self.check_fail("kg_timeline")?;
            Ok(self.kg_timeline_response.clone())
        }

        async fn kg_stats(&self) -> mempalace_remote::Result<Value> {
            self.check_fail("kg_stats")?;
            Ok(self.kg_stats_response.clone())
        }

        async fn taxonomy(&self) -> mempalace_remote::Result<Value> {
            self.check_fail("taxonomy")?;
            Ok(self.taxonomy.clone())
        }

        async fn wings(&self) -> mempalace_remote::Result<Value> {
            self.check_fail("wings")?;
            Ok(self.wings.clone())
        }

        async fn rooms(&self, _wing: Option<&str>) -> mempalace_remote::Result<Value> {
            self.check_fail("rooms")?;
            Ok(self.rooms.clone())
        }

        async fn changes(&self, query: ChangesQuery) -> mempalace_remote::Result<ChangesResponse> {
            self.check_fail("changes")?;
            // Record the cursor we received so tests can assert passthrough.
            *self.received_cursor.lock().unwrap() = query.cursor;
            Ok(ChangesResponse {
                events: self.changes_events.clone(),
                next_cursor: self.changes_next_cursor.clone(),
            })
        }

        async fn ingest_batch(
            &self,
            _req: mempalace_federation::IngestBatchRequest,
        ) -> mempalace_remote::Result<mempalace_federation::IngestBatchResponse> {
            self.check_fail("ingest_batch")?;
            Ok(mempalace_federation::IngestBatchResponse { files: vec![], warnings: vec![] })
        }

        async fn coordination_task_get(
            &self,
            task_id: &str,
        ) -> mempalace_remote::Result<mempalace_federation::CoordinationTaskDto> {
            self.coordination_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = task_id;
            self.coordination_task_get_outcome.clone().into_task_get_result()
        }

        async fn coordination_task_claim(
            &self,
            task_id: &str,
            req: TaskLeaseRequest,
        ) -> mempalace_remote::Result<
            RemoteRevisionedWrite<mempalace_federation::CoordinationTaskDto>,
        > {
            self.coordination_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = (task_id, req);
            self.coordination_task_write_outcome.clone().into_task_write_result()
        }

        async fn coordination_task_renew(
            &self,
            task_id: &str,
            req: TaskLeaseRequest,
        ) -> mempalace_remote::Result<
            RemoteRevisionedWrite<mempalace_federation::CoordinationTaskDto>,
        > {
            self.coordination_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = (task_id, req);
            self.coordination_task_write_outcome.clone().into_task_write_result()
        }

        async fn coordination_task_transition(
            &self,
            task_id: &str,
            req: TransitionTaskRequest,
        ) -> mempalace_remote::Result<
            RemoteRevisionedWrite<mempalace_federation::CoordinationTaskDto>,
        > {
            self.coordination_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = (task_id, req);
            self.coordination_task_write_outcome.clone().into_task_write_result()
        }

        async fn coordination_message_get(
            &self,
            message_id: &str,
        ) -> mempalace_remote::Result<mempalace_federation::CoordinationMessageDto> {
            self.coordination_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = message_id;
            Err(RemoteError::RemoteRejected {
                remote: "mock".to_owned(),
                status: 404,
                body: "not found".to_owned(),
            })
        }
    }

    impl MockRemote {
        fn check_fail(&self, endpoint: &str) -> mempalace_remote::Result<()> {
            if self.fail_on.as_deref() == Some(endpoint) {
                Err(RemoteError::Unreachable {
                    remote: "mock".to_owned(),
                    message: "mock failure".to_owned(),
                })
            } else if self.fail_unknown_on.as_deref() == Some(endpoint) {
                Err(RemoteError::UnknownOutcome {
                    remote: "mock".to_owned(),
                    message: format!("mock {endpoint} outcome lost"),
                })
            } else {
                Ok(())
            }
        }
    }

    fn make_resolved_remote(name: &str) -> ResolvedRemote {
        use std::time::Duration;
        ResolvedRemote {
            name: name.to_owned(),
            url: "https://test.example".to_owned(),
            token: None,
            timeout: Duration::from_secs(5),
        }
    }

    fn make_router(remotes: BTreeMap<String, Arc<dyn RemoteApi>>) -> FederationRouter {
        let mut rules_remotes = BTreeMap::new();
        for name in remotes.keys() {
            rules_remotes.insert(name.clone(), make_resolved_remote(name));
        }
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Combined,
            default_remote: remotes.keys().next().cloned(),
            wings: BTreeMap::new(),
            kg: Some(ResolvedRouteRule {
                mode: RouteMode::Combined,
                remote: remotes.keys().next().cloned(),
                write: WriteTarget::Remote,
            }),
            coordination: BTreeMap::new(),
        };
        FederationRouter::with_remotes(rules, remotes)
    }

    fn make_combined_route(remote_name: &str) -> ResolvedRouteRule {
        ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some(remote_name.to_owned()),
            write: WriteTarget::Remote,
        }
    }

    fn make_both_route(remote_name: &str) -> ResolvedRouteRule {
        ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some(remote_name.to_owned()),
            write: WriteTarget::Both,
        }
    }

    // ── Coordination federation gate (defect fix) ───────────────────────────────

    /// A local coordination miss must not touch a configured remote at all when coordination
    /// federation was never configured — no `federation.coordination` entry, and
    /// `default_mode` is `Local` (the field's own default, so this is also what an operator
    /// gets by only ever setting `federation.remotes` and, say, a `federation.wings` entry for
    /// drawers). Before this fix, `coordination_read_fallback`/`coordination_write_fallback`
    /// (and the claim/renew/transition equivalents) iterated `self.remotes` unconditionally,
    /// so *any* configured remote — regardless of what it was configured for — would receive
    /// every local coordination miss.
    ///
    /// Asserting the result is `None`/`Ok(None)` alone would not prove anything: a genuine
    /// remote miss returns exactly the same shape after a real round trip. This asserts the
    /// stronger, actually-diagnostic property — the mock's `coordination_calls` counter, bumped
    /// by every `coordination_*` method it implements, must stay at zero.
    #[tokio::test]
    async fn coordination_fallback_records_zero_remote_calls_without_coordination_federation_config()
     {
        let mock = MockRemote::default();
        let calls = std::sync::Arc::clone(&mock.coordination_calls);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);

        let mut rules_remotes = BTreeMap::new();
        for name in remotes.keys() {
            rules_remotes.insert(name.clone(), make_resolved_remote(name));
        }
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings: BTreeMap::new(),
            kg: None,
            coordination: BTreeMap::new(),
        };
        assert!(
            rules.coordination.is_empty() && rules.default_mode == RouteMode::Local,
            "test setup must actually leave coordination federation unconfigured"
        );
        let router = FederationRouter::with_remotes(rules, remotes);
        assert!(
            !router.coordination_federation_enabled(),
            "coordination federation must read as disabled under this config"
        );

        // Reads: task get, message get.
        assert_eq!(router.coordination_task_get_fallback("task_missing").await.unwrap(), None);
        assert_eq!(router.coordination_message_get_fallback("msg_missing").await.unwrap(), None);

        // ID-referencing writes: claim, and message send (via coordination_write_fallback).
        let claim = router
            .coordination_task_claim_fallback(
                "task_missing",
                TaskLeaseRequest {
                    expected_revision: 0,
                    lease_seconds: 60,
                    worker: Some("worker-a".to_owned()),
                },
            )
            .await
            .expect("gate must short-circuit before any remote call, not error");
        assert_eq!(claim, None);

        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "zero coordination calls must have reached the mock remote"
        );
    }

    // ── Coordination read-fallback error policy (Codex finding 1) ───────────────

    /// A default-mode-Combined single-remote router: coordination federation reads as enabled,
    /// and `remote_name` is both the only configured remote and (via `default_remote`) the only
    /// coordination candidate.
    fn make_single_remote_coordination_router(
        remote_name: &str,
        mock: MockRemote,
    ) -> FederationRouter {
        let mut remotes: BTreeMap<String, Arc<dyn RemoteApi>> = BTreeMap::new();
        remotes.insert(remote_name.to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let mut rules_remotes = BTreeMap::new();
        rules_remotes.insert(remote_name.to_owned(), make_resolved_remote(remote_name));
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Combined,
            default_remote: Some(remote_name.to_owned()),
            wings: BTreeMap::new(),
            kg: None,
            coordination: BTreeMap::new(),
        };
        FederationRouter::with_remotes(rules, remotes)
    }

    /// `Unauthorized` from the only candidate remote must surface as a hard `ToolError`, not a
    /// silent `found: false` — a caller cannot otherwise tell "the record does not exist" apart
    /// from "your token is wrong". This is Codex finding 1 (id 3832912257): before the fix,
    /// `coordination_read_fallback` logged and swallowed every error, including this one.
    #[tokio::test]
    async fn coordination_read_fallback_surfaces_unauthorized_as_hard_error() {
        let mut mock = MockRemote::default();
        mock.coordination_task_get_outcome = MockCoordOutcome::Unauthorized;
        let router = make_single_remote_coordination_router("alpha", mock);

        let err = router
            .coordination_task_get_fallback("task_missing")
            .await
            .expect_err("Unauthorized must be a hard error, not a swallowed miss");
        let msg = format!("{err:?}");
        assert!(msg.contains("alpha"), "error must name the offending remote: {msg}");
    }

    /// `CapabilityMissing` from the only candidate remote means it definitively does not
    /// implement coordination at all — the same "not this palace" case a `404` is, not a
    /// misconfiguration — so a read must skip it and come back as a plain miss, matching
    /// `coordination_write_fallback`'s (correct) handling of the same error. This inverts what
    /// was `coordination_read_fallback_surfaces_capability_missing_as_hard_error`: that test
    /// encoded the live regression where `e3fa83b` reconciled `CapabilityMissing` in opposite
    /// directions on the read and write sides, hard-erroring every coordination read (task_get,
    /// message_get, artifact_get, result_get) against a `combined`-mode remote that simply
    /// predates coordination support, instead of returning `found: false`.
    #[tokio::test]
    async fn coordination_read_fallback_skips_capability_missing_as_definitive_miss() {
        let mut mock = MockRemote::default();
        mock.coordination_task_get_outcome = MockCoordOutcome::CapabilityMissing;
        let router = make_single_remote_coordination_router("alpha", mock);

        let result = router
            .coordination_task_get_fallback("task_missing")
            .await
            .expect("CapabilityMissing must be skipped, not a hard error, for a read");
        assert_eq!(result, None, "a remote with no coordination support must read as a miss");
    }

    /// Same as above for `VersionSkew`.
    #[tokio::test]
    async fn coordination_read_fallback_surfaces_version_skew_as_hard_error() {
        let mut mock = MockRemote::default();
        mock.coordination_task_get_outcome = MockCoordOutcome::VersionSkew;
        let router = make_single_remote_coordination_router("alpha", mock);

        router
            .coordination_task_get_fallback("task_missing")
            .await
            .expect_err("VersionSkew must be a hard error for a read");
    }

    /// Same as above for `InvalidResponse` (an undecodable 2xx body) — also terminal for a read.
    #[tokio::test]
    async fn coordination_read_fallback_surfaces_invalid_response_as_hard_error() {
        let mut mock = MockRemote::default();
        mock.coordination_task_get_outcome = MockCoordOutcome::InvalidResponse;
        let router = make_single_remote_coordination_router("alpha", mock);

        router
            .coordination_task_get_fallback("task_missing")
            .await
            .expect_err("InvalidResponse must be a hard error for a read");
    }

    /// A genuine 404 ("not this palace") must still be treated as a plain miss, not an error —
    /// this is the "do not turn every miss into an error" half of finding 1: only widen the
    /// error surface for the genuinely-broken cases, not for an honest "record not found".
    #[tokio::test]
    async fn coordination_read_fallback_genuine_404_miss_stays_found_false() {
        let mock = MockRemote::default(); // default outcome is NotFound (404)
        let router = make_single_remote_coordination_router("alpha", mock);

        let result = router
            .coordination_task_get_fallback("task_missing")
            .await
            .expect("a plain 404 must not become an error");
        assert_eq!(result, None, "a genuine miss must come back as Ok(None), not an error");
    }

    /// An `Unreachable` remote is the one genuinely degradable read failure and must not block
    /// discovery — this is the read/write asymmetry the finding calls out explicitly.
    #[tokio::test]
    async fn coordination_read_fallback_unreachable_remote_is_skipped_not_errored() {
        let mut mock = MockRemote::default();
        mock.coordination_task_get_outcome = MockCoordOutcome::Unreachable;
        let router = make_single_remote_coordination_router("alpha", mock);

        let result = router
            .coordination_task_get_fallback("task_missing")
            .await
            .expect("an unreachable remote must degrade, not hard-fail a read");
        assert_eq!(result, None);
    }

    // ── Coordination write-discovery candidate set + CapabilityMissing (Codex finding 2) ────

    /// Reproduces Codex finding 2 (id 3832912200) exactly: two remotes, with the drawers-only
    /// one sorting FIRST in `BTreeMap` iteration order ("alpha" < "zeta"). Only "zeta" is wired
    /// up for coordination (via a `federation.coordination` rule); "alpha" is a remote that
    /// exists only for other purposes and does not support coordination at all
    /// (`CapabilityMissing`). A claim for a task that lives on "zeta" must still reach it —
    /// before the fix, "alpha" sorting first and returning `CapabilityMissing` (a non-404 error)
    /// terminated the search before "zeta" was ever tried, exactly as the finding describes. A
    /// single-remote test would never have caught this; two are required.
    #[tokio::test]
    async fn coordination_write_fallback_reaches_later_remote_past_earlier_capability_missing() {
        let mut alpha = MockRemote::default();
        alpha.coordination_task_write_outcome = MockCoordOutcome::CapabilityMissing;
        let mut zeta = MockRemote::default();
        let expected_task = make_task_dto("task-1");
        zeta.coordination_task_write_outcome =
            MockCoordOutcome::Applied(Box::new(expected_task.clone()));
        let zeta_calls = std::sync::Arc::clone(&zeta.coordination_calls);

        let mut remotes: BTreeMap<String, Arc<dyn RemoteApi>> = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(alpha) as Arc<dyn RemoteApi>);
        remotes.insert("zeta".to_owned(), Arc::new(zeta) as Arc<dyn RemoteApi>);
        assert_eq!(
            remotes.keys().collect::<Vec<_>>(),
            vec!["alpha", "zeta"],
            "test setup must actually put the drawers-only remote first in iteration order"
        );

        let mut rules_remotes = BTreeMap::new();
        rules_remotes.insert("alpha".to_owned(), make_resolved_remote("alpha"));
        rules_remotes.insert("zeta".to_owned(), make_resolved_remote("zeta"));
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_team".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("zeta".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let mut wings = BTreeMap::new();
        wings.insert(
            "wing_drawers".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings,
            kg: None,
            coordination,
        };
        let router = FederationRouter::with_remotes(rules, remotes);

        let value = router
            .coordination_task_claim_fallback(
                "task-1",
                TaskLeaseRequest {
                    expected_revision: 0,
                    lease_seconds: 60,
                    worker: Some("worker-1".to_owned()),
                },
            )
            .await
            .expect("must not hard-error")
            .expect("claim must reach zeta and succeed");

        assert_eq!(value["applied_to"], "remote:zeta", "the claim must land on zeta: {value}");
        assert_eq!(
            zeta_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "zeta must actually have been called"
        );
        assert_eq!(value["task"]["task_id"], "task-1");
    }

    /// A `RemoteApi` implementor that inherits every coordination method's trait default (i.e.
    /// implements none of them itself) — the exact shape the `RemoteApi` trait-level comment in
    /// `mempalace-remote` describes as "not reachable through `RemoteClient` today, but live for
    /// any other implementor." Every non-coordination method is implemented since the trait has
    /// no defaults for those; every coordination method is deliberately left to the default body.
    struct DefaultsOnlyRemote;

    #[async_trait::async_trait]
    impl RemoteApi for DefaultsOnlyRemote {
        async fn info(&self) -> mempalace_remote::Result<InfoResponse> {
            Ok(serde_json::from_value(json!({
                "server_version": "1.0.0-test",
                "federation_api_version": 1,
                "embedding_profile": "balanced",
                "capabilities": ["drawers"]
            }))
            .unwrap())
        }
        async fn search_drawers(
            &self,
            _req: DrawerSearchRequest,
        ) -> mempalace_remote::Result<DrawerSearchResponse> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn check_duplicate(
            &self,
            _req: CheckDuplicateRequest,
        ) -> mempalace_remote::Result<CheckDuplicateResponse> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn add_drawer(
            &self,
            _req: AddDrawerRequest,
        ) -> mempalace_remote::Result<AddDrawerResponse> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn list_drawers(
            &self,
            _query: ListDrawersQuery,
        ) -> mempalace_remote::Result<ListDrawersResponse> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn get_drawer(&self, _drawer_id: &str) -> mempalace_remote::Result<Value> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn delete_drawer(&self, _drawer_id: &str) -> mempalace_remote::Result<()> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn kg_query(&self, _req: KgQueryRequest) -> mempalace_remote::Result<Value> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn kg_add_fact(&self, _req: KgAddFactRequest) -> mempalace_remote::Result<Value> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn kg_invalidate(
            &self,
            _req: KgInvalidateRequest,
        ) -> mempalace_remote::Result<Value> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn kg_timeline(&self, _entity: Option<&str>) -> mempalace_remote::Result<Value> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn kg_stats(&self) -> mempalace_remote::Result<Value> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn taxonomy(&self) -> mempalace_remote::Result<Value> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn wings(&self) -> mempalace_remote::Result<Value> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn rooms(&self, _wing: Option<&str>) -> mempalace_remote::Result<Value> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn changes(&self, _query: ChangesQuery) -> mempalace_remote::Result<ChangesResponse> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
        async fn ingest_batch(
            &self,
            _req: mempalace_federation::IngestBatchRequest,
        ) -> mempalace_remote::Result<mempalace_federation::IngestBatchResponse> {
            unimplemented!("not exercised by the coordination-defaults test")
        }
    }

    /// Regression test for the `RemoteApi` trait-default fix: a candidate remote that has not
    /// overridden any coordination method (and therefore hits the shared
    /// `coordination_unsupported` default body) must be *skipped* by the write fallback in
    /// favour of the next candidate, not treated as a terminal error that aborts the whole
    /// search. Before the fix, the default returned a synthetic-501 `RemoteRejected`, which
    /// matches neither write fallback skip condition (`404` or `CapabilityMissing`) and so was
    /// terminal — this test fails against that old behaviour and passes once the default returns
    /// `CapabilityMissing` instead.
    #[tokio::test]
    async fn coordination_write_fallback_skips_a_trait_default_implementor_instead_of_failing_hard()
    {
        let defaults_only: Arc<dyn RemoteApi> = Arc::new(DefaultsOnlyRemote);
        let mut real = MockRemote::default();
        let expected_task = make_task_dto("task-1");
        real.coordination_task_write_outcome =
            MockCoordOutcome::Applied(Box::new(expected_task.clone()));
        let real_calls = std::sync::Arc::clone(&real.coordination_calls);

        let mut remotes: BTreeMap<String, Arc<dyn RemoteApi>> = BTreeMap::new();
        remotes.insert("alpha-defaults".to_owned(), defaults_only);
        remotes.insert("zeta-real".to_owned(), Arc::new(real) as Arc<dyn RemoteApi>);
        assert_eq!(
            remotes.keys().collect::<Vec<_>>(),
            vec!["alpha-defaults", "zeta-real"],
            "test setup must put the defaults-only remote first in iteration order"
        );

        let mut rules_remotes = BTreeMap::new();
        rules_remotes.insert("alpha-defaults".to_owned(), make_resolved_remote("alpha-defaults"));
        rules_remotes.insert("zeta-real".to_owned(), make_resolved_remote("zeta-real"));
        let mut coordination = BTreeMap::new();
        coordination.insert(
            "wing_team".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha-defaults".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        coordination.insert(
            "wing_other".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("zeta-real".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings: BTreeMap::new(),
            kg: None,
            coordination,
        };
        let router = FederationRouter::with_remotes(rules, remotes);

        let value = router
            .coordination_task_claim_fallback(
                "task-1",
                TaskLeaseRequest {
                    expected_revision: 0,
                    lease_seconds: 60,
                    worker: Some("worker-1".to_owned()),
                },
            )
            .await
            .expect("the defaults-only remote must be skipped, not terminate the search")
            .expect("claim must reach zeta-real and succeed");

        assert_eq!(
            value["applied_to"], "remote:zeta-real",
            "the claim must land on the real remote past the defaults-only one: {value}"
        );
        assert_eq!(
            real_calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "zeta-real must actually have been called"
        );
    }

    /// A revision conflict from the sole candidate remote must still surface via the shared
    /// `revision_conflict_payload` shape after the read/write-fallback refactor that merged
    /// `coordination_task_transition_fallback` into the generic
    /// `coordination_task_revisioned_fallback` — a regression here would mean the merge changed
    /// behaviour, not just structure.
    #[tokio::test]
    async fn coordination_task_write_conflict_surfaces_revision_conflict_payload() {
        let mut mock = MockRemote::default();
        mock.coordination_task_write_outcome = MockCoordOutcome::Conflict(Some(5));
        let router = make_single_remote_coordination_router("alpha", mock);

        let value = router
            .coordination_task_claim_fallback(
                "task-conflict",
                TaskLeaseRequest {
                    expected_revision: 2,
                    lease_seconds: 60,
                    worker: Some("worker-1".to_owned()),
                },
            )
            .await
            .expect("must not hard-error")
            .expect("conflict must still produce a value");

        assert_eq!(value["success"], false);
        assert_eq!(value["conflict"]["expected_revision"], 2);
        assert_eq!(value["conflict"]["actual_revision"], 5);
    }

    // ── Remote task-write envelope must nest under `task` (Codex finding 3) ─────────────────

    /// The remote claim/renew/transition fallbacks must nest the task DTO under `"task"`,
    /// matching the local success shape `{"success": true, "task": {...}}` exactly — see
    /// `McpRuntime::tool_task_claim` in `lib.rs`. Before the fix, the DTO was flattened at the
    /// top level (`{"task_id": ..., "success": true, "applied_to": ...}`), which loses the task
    /// entirely for a client that deserialises the (now-documented) local shape.
    #[tokio::test]
    async fn coordination_task_write_fallbacks_nest_task_under_task_key() {
        let task = make_task_dto("task-envelope");

        let mut mock_claim = MockRemote::default();
        mock_claim.coordination_task_write_outcome =
            MockCoordOutcome::Applied(Box::new(task.clone()));
        let claim_router = make_single_remote_coordination_router("alpha", mock_claim);
        let claim = claim_router
            .coordination_task_claim_fallback(
                "task-envelope",
                TaskLeaseRequest {
                    expected_revision: 0,
                    lease_seconds: 60,
                    worker: Some("worker-1".to_owned()),
                },
            )
            .await
            .expect("must not error")
            .expect("must find a value");

        let mut mock_renew = MockRemote::default();
        mock_renew.coordination_task_write_outcome =
            MockCoordOutcome::Applied(Box::new(task.clone()));
        let renew_router = make_single_remote_coordination_router("alpha", mock_renew);
        let renew = renew_router
            .coordination_task_renew_fallback(
                "task-envelope",
                TaskLeaseRequest {
                    expected_revision: 1,
                    lease_seconds: 60,
                    worker: Some("worker-1".to_owned()),
                },
            )
            .await
            .expect("must not error")
            .expect("must find a value");

        let mut mock_transition = MockRemote::default();
        mock_transition.coordination_task_write_outcome =
            MockCoordOutcome::Applied(Box::new(task.clone()));
        let transition_router = make_single_remote_coordination_router("alpha", mock_transition);
        let transition = transition_router
            .coordination_task_transition_fallback(
                "task-envelope",
                TransitionTaskRequest {
                    expected_revision: 1,
                    state: mempalace_federation::CoordinationTaskState::Completed,
                    actor: Some("worker-1".to_owned()),
                    details: None,
                },
            )
            .await
            .expect("must not error")
            .expect("must find a value");

        for (name, value) in [("claim", &claim), ("renew", &renew), ("transition", &transition)] {
            assert_eq!(value["success"], true, "{name}: envelope must report success");
            assert!(
                value.get("task_id").is_none(),
                "{name}: the task must not be flattened at the top level: {value}"
            );
            let nested = value.get("task").unwrap_or_else(|| {
                panic!("{name}: envelope must carry a non-empty `task`: {value}")
            });
            assert_eq!(
                nested["task_id"], "task-envelope",
                "{name}: task fields must be reachable under `task`"
            );
            assert_eq!(
                nested["wing"], "wing_team",
                "{name}: task fields must be reachable under `task`"
            );
            assert_eq!(
                value["applied_to"], "remote:alpha",
                "{name}: applied_to stays at envelope level"
            );
        }
    }

    /// Local and remote must agree on the success envelope shape so a client cannot tell (and
    /// does not need to special-case) whether a claim was served locally or via remote
    /// fallback. Drives the identical assertion helper over both a local-shaped value (built the
    /// way `tool_task_claim` builds it) and the remote fallback's output, so the two shapes
    /// cannot silently drift apart again.
    #[tokio::test]
    async fn coordination_task_write_local_and_remote_envelopes_match() {
        fn assert_claim_envelope_shape(value: &Value, expected_task_id: &str) {
            assert_eq!(value["success"], true);
            assert!(value.get("task_id").is_none(), "task must be nested, not flattened");
            let task = value.get("task").expect("envelope must carry `task`");
            assert_eq!(task["task_id"], expected_task_id);
        }

        // Local shape, built exactly the way `tool_task_claim` builds
        // `{"success": true, "task": task}` in lib.rs.
        let local_task = make_task_dto("task-shared");
        let local_value = json!({"success": true, "task": local_task});
        assert_claim_envelope_shape(&local_value, "task-shared");

        // Remote shape, via the real fallback path.
        let mut mock = MockRemote::default();
        mock.coordination_task_write_outcome =
            MockCoordOutcome::Applied(Box::new(make_task_dto("task-shared")));
        let router = make_single_remote_coordination_router("alpha", mock);
        let remote_value = router
            .coordination_task_claim_fallback(
                "task-shared",
                TaskLeaseRequest {
                    expected_revision: 0,
                    lease_seconds: 60,
                    worker: Some("worker-1".to_owned()),
                },
            )
            .await
            .expect("must not error")
            .expect("must find a value");
        assert_claim_envelope_shape(&remote_value, "task-shared");
    }

    // ── add_drawer_replicate tests ──────────────────────────────────────────────

    #[tokio::test]
    async fn e2e_add_drawer_remote_skips_work_for_both() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let result = router
            .add_drawer_remote("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "Both route should not produce remote result from add_drawer_remote"
        );
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_succeeds() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router
            .add_drawer_replicate("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await;

        assert_eq!(status, ReplicationStatus::Replicated { remote: "alpha".to_owned() });
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_duplicate_remote() {
        let mut mock = MockRemote::default();
        mock.duplicate_matches = vec![json!({"drawer_id":"dup-1","similarity":0.95})];
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router
            .add_drawer_replicate("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await;

        match status {
            ReplicationStatus::Failed { remote, .. } => {
                assert_eq!(remote, "alpha");
            }
            other => panic!("expected Failed (similarity-only), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_converged_exact_match() {
        let content = "exact match content";
        let content_hash = hash_text(content);
        let mut mock = MockRemote::default();
        mock.duplicate_matches = vec![json!({
            "drawer_id": "exact-1",
            "similarity": 1.0,
            "content_hash": content_hash,
        })];
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status =
            router.add_drawer_replicate("w", "r", content, "file.txt", "agent", &route, 0.9).await;

        assert_eq!(status, ReplicationStatus::Converged { remote: "alpha".to_owned() });
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_remote_failure() {
        let mut mock = MockRemote::default();
        mock.fail_on = Some("add_drawer".to_owned());
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router
            .add_drawer_replicate("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await;

        match status {
            ReplicationStatus::Failed { remote, .. } => {
                assert_eq!(remote, "alpha");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_409_race() {
        let mut mock = MockRemote::default();
        mock.duplicate_matches = vec![]; // pre-check passes (no duplicate detected)
        mock.add_drawer_409 = true; // but the actual add_drawer gets a 409
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router
            .add_drawer_replicate("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await;

        match status {
            ReplicationStatus::Failed { remote, reason } => {
                assert_eq!(remote, "alpha");
                assert_eq!(reason, "duplicate (409) on remote");
            }
            other => panic!("expected Failed (409 race), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_skipped_for_non_both() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha"); // write:remote
        let status = router
            .add_drawer_replicate("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await;

        assert_eq!(status, ReplicationStatus::Skipped);
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_skipped_for_diary_wing() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router
            .add_drawer_replicate(
                SHARED_AGENT_DIARY_WING,
                "r",
                "diary content",
                "diary:general",
                "agent",
                &route,
                0.9,
            )
            .await;

        assert_eq!(status, ReplicationStatus::Skipped, "diary wing should not replicate");
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_skipped_for_diary_room() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router
            .add_drawer_replicate(
                "wing_code",
                DIARY_ROOM,
                "diary content",
                "diary:general",
                "agent",
                &route,
                0.9,
            )
            .await;

        assert_eq!(status, ReplicationStatus::Skipped, "diary room should not replicate");
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_skipped_for_diary_source_file() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router
            .add_drawer_replicate(
                "wing_code",
                "room",
                "diary content",
                "diary:standup",
                "agent",
                &route,
                0.9,
            )
            .await;

        assert_eq!(status, ReplicationStatus::Skipped, "diary source_file should not replicate");
    }

    #[tokio::test]
    async fn e2e_add_drawer_replicate_succeeds_with_real_route_via_write_intent() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        // Verify that is_dual_write correctly identifies both routes.
        let both_route = make_both_route("alpha");
        assert!(router.is_dual_write(&both_route));

        // Verify resolve_write_target returns the expected variants.
        let remote_route = make_combined_route("alpha");
        assert_eq!(router.resolve_write_target(&remote_route), WriteTarget::Remote);

        let local_route =
            ResolvedRouteRule { mode: RouteMode::Local, remote: None, write: WriteTarget::Local };
        assert_eq!(router.resolve_write_target(&local_route), WriteTarget::Local);

        assert_eq!(router.resolve_write_target(&both_route), WriteTarget::Both);
    }

    #[tokio::test]
    async fn e2e_search_merges_and_annotates_origin() {
        let mut mock = MockRemote::default();
        mock.search_results =
            vec![json!({"wing":"w","room":"r2","similarity":0.8,"text":"remote hit"})];
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = vec![json!({"wing":"w","room":"r1","similarity":0.9,"text":"local hit"})];
        let result = router
            .search(local, "test", Some("w"), None, None, 10, &["alpha".to_owned()])
            .await
            .unwrap();

        let results = result["results"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        // Interleave: local first, then remote
        assert_eq!(results[0]["text"], "local hit");
        assert!(!results[0]["origin"].is_null());
        assert_eq!(results[1]["text"], "remote hit");
        assert_eq!(results[1]["origin"], "alpha");
    }

    #[tokio::test]
    async fn search_forwards_view_to_remote() {
        let mock = MockRemote::default();
        let received_search_view = Arc::clone(&mock.received_search_view);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        router
            .search(vec![], "test", Some("w"), None, Some("feature-x"), 10, &["alpha".to_owned()])
            .await
            .unwrap();

        assert_eq!(*received_search_view.lock().unwrap(), Some("feature-x".to_owned()));
    }

    #[tokio::test]
    async fn search_returns_remote_path_for_local_overlay_filtering() {
        let mut mock = MockRemote::default();
        mock.search_results = vec![json!({
            "wing":"remote_wing",
            "room":"r",
            "source_file":"README.md",
            "similarity":0.8,
            "text":"remote hit"
        })];
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);
        let result = router
            .search(vec![], "test", None, None, Some("feature-x"), 10, &["alpha".to_owned()])
            .await
            .unwrap();

        assert_eq!(result["results"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn e2e_search_degradable_on_remote_outage() {
        let mut mock = MockRemote::default();
        mock.fail_on = Some("search".to_owned());
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = vec![json!({"wing":"w","room":"r1","similarity":0.9,"text":"local only"})];
        let result = router
            .search(local, "test", Some("w"), None, None, 10, &["alpha".to_owned()])
            .await
            .unwrap();

        let results = result["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["text"], "local only");
        let warnings = result["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 1);
        // Use Display format (not "unreachable"); check it mentions the remote name
        assert!(warnings[0].as_str().unwrap().contains("alpha"));
        // Structured degradation accompanies the legacy string warning.
        let degradations = result["degradations"].as_array().unwrap();
        assert_eq!(degradations.len(), 1);
        assert_eq!(degradations[0]["code"], "remote_read_degraded");
        assert_eq!(degradations[0]["remote"], "alpha");
        assert_eq!(degradations[0]["kind"], "search");
        assert_eq!(degradations[0]["classification"], "unreachable");
        assert!(!degradations[0]["error"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn e2e_add_drawer_routes_to_remote() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result["success"], true);
        assert_eq!(result["origin"], "alpha");
        assert_eq!(result["applied_to"], "remote:alpha");
        assert_eq!(result["drawer_id"], "rem-drawer-1");
    }

    #[tokio::test]
    async fn e2e_add_drawer_pre_check_duplicate_returns_matches_with_origin() {
        let mut mock = MockRemote::default();
        mock.duplicate_matches = vec![json!({"drawer_id":"dup-1","similarity":0.95})];
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result["success"], false);
        assert_eq!(result["reason"], "duplicate");
        assert_eq!(result["origin"], "alpha");
        assert_eq!(result["applied_to"], "remote:alpha");
        let matches = result["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["origin"], "alpha");
    }

    #[tokio::test]
    async fn e2e_add_drawer_409_race_returns_duplicate_shape() {
        let mut mock = MockRemote::default();
        mock.add_drawer_409 = true;
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(result["success"], false);
        assert_eq!(result["reason"], "duplicate");
        assert_eq!(result["origin"], "alpha");
        assert_eq!(result["applied_to"], "remote:alpha");
        let matches = result["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 0);
    }

    #[tokio::test]
    async fn e2e_add_drawer_operation_conflict_is_not_reported_as_duplicate() {
        let mut mock = MockRemote::default();
        mock.add_drawer_409 = true;
        mock.add_drawer_409_body = "operation_id_conflict: operation already used".to_owned();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let error = router
            .add_drawer_remote_with_operation(
                "w",
                "r",
                "content",
                "file.txt",
                "agent",
                &route,
                0.9,
                Some("op-conflict-1"),
            )
            .await
            .expect_err("operation-id conflict must remain an authoritative error");

        match error {
            ToolError::Internal(McpError::Federation(message)) => {
                assert!(message.contains("operation_id_conflict"));
                assert!(!message.contains("reason: duplicate"));
            }
            other => panic!("expected federation error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn replay_commit_then_lost_response_bypasses_preflight_and_reaches_receipt_replay() {
        // Regression for #127: the first operation-aware attempt commits remotely but loses its
        // response (UnknownOutcome). Retrying the SAME operation_id must bypass the client-side
        // semantic preflight and re-send the mutation so the receiving receipt store authoritatively
        // replays it — otherwise the preflight would short-circuit on the now-committed duplicate and
        // never reach the replay.
        let mut mock = MockRemote::default();
        // A legacy preflight WOULD report this content as a duplicate of the committed attempt.
        mock.duplicate_matches = vec![json!({"drawer_id": "rem-drawer-1", "similarity": 0.99})];
        mock.add_drawer_commit_then_unknown = true;
        let check_duplicate_calls = std::sync::Arc::clone(&mock.check_duplicate_calls);
        let received_ids = std::sync::Arc::clone(&mock.received_add_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");

        // First attempt: committed but response lost.
        let first = router
            .add_drawer_remote_with_operation(
                "w",
                "r",
                "content",
                "file.txt",
                "agent",
                &route,
                0.9,
                Some("op-replay-1"),
            )
            .await
            .unwrap()
            .expect("routed remote add must return a result");
        assert_eq!(first["outcome"], "unknown_outcome");
        assert_eq!(first["success"], false);
        assert_eq!(first["operation_id"], "op-replay-1");

        // Retry with the same operation_id: the receipt store replay returns the original result.
        let second = router
            .add_drawer_remote_with_operation(
                "w",
                "r",
                "content",
                "file.txt",
                "agent",
                &route,
                0.9,
                Some("op-replay-1"),
            )
            .await
            .unwrap()
            .expect("routed remote add retry must return a result");
        assert_eq!(second["success"], true);
        assert_eq!(second["drawer_id"], "rem-drawer-1");

        // Both attempts reached the remote carrying the SAME stable operation_id...
        let seen = received_ids.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[Some("op-replay-1".to_owned()), Some("op-replay-1".to_owned())]
        );
        drop(seen);
        // ...and the semantic preflight was NEVER invoked for either operation-aware attempt.
        assert_eq!(
            check_duplicate_calls.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "operation-aware retries must bypass the client preflight"
        );
    }

    #[tokio::test]
    async fn add_drawer_unknown_outcome_returns_structured_result() {
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("add_drawer".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_add_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote_with_operation(
                "w",
                "r",
                "content",
                "file.txt",
                "agent",
                &route,
                0.9,
                Some("op-unknown-1"),
            )
            .await
            .unwrap()
            .expect("unknown outcome must still return a structured result, not an error");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["success"], false);
        assert_eq!(result["remote"], "mock");
        assert_eq!(result["operation_id"], "op-unknown-1");
        assert!(result["error"].as_str().unwrap().contains("outcome lost"));
        assert!(
            result["retry"].as_str().unwrap().contains("same operation_id"),
            "structured result must carry safe-retry guidance"
        );
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.as_slice(), &[Some("op-unknown-1".to_owned())]);
    }

    #[tokio::test]
    async fn add_drawer_omitted_operation_id_generates_and_returns_same_id_on_unknown_outcome() {
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("add_drawer".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_add_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote_with_operation(
                "w", "r", "content", "file.txt", "agent", &route, 0.9, None,
            )
            .await
            .unwrap()
            .expect("unknown outcome must return a structured result");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["success"], false);
        assert_eq!(result["remote"], "mock");
        let op_id =
            result["operation_id"].as_str().expect("generated operation_id must be a string");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");
        assert!(result["error"].as_str().unwrap().contains("outcome lost"));
        assert!(result["retry"].as_str().unwrap().contains("same operation_id"));

        let seen = received_ids.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[Some(op_id.to_owned())],
            "remote must receive the exact generated operation_id"
        );
    }

    #[tokio::test]
    async fn routed_remote_delete_unknown_outcome_returns_structured_result() {
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("delete".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_delete_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .delete_drawer_routed_remote("d-1", &route, Some("op-del-unknown-1"))
            .await
            .unwrap()
            .expect("unknown delete outcome must return a structured result");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["success"], false);
        assert_eq!(result["remote"], "mock");
        assert_eq!(result["operation_id"], "op-del-unknown-1");
        assert!(result["retry"].as_str().unwrap().contains("same operation_id"));
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.as_slice(), &[Some("op-del-unknown-1".to_owned())]);
    }

    #[tokio::test]
    async fn routed_delete_drawer_omitted_operation_id_generates_and_returns_same_id_on_unknown_outcome()
     {
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("delete".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_delete_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .delete_drawer_routed_remote("d-1", &route, None)
            .await
            .unwrap()
            .expect("unknown delete outcome must return a structured result");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["success"], false);
        assert_eq!(result["remote"], "mock");
        let op_id =
            result["operation_id"].as_str().expect("generated operation_id must be a string");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");
        assert!(result["retry"].as_str().unwrap().contains("same operation_id"));

        let seen = received_ids.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[Some(op_id.to_owned())],
            "remote must receive the exact generated operation_id"
        );
    }

    #[tokio::test]
    async fn all_remote_delete_unknown_outcome_is_not_swallowed_as_not_found() {
        // The all-remote fallback must surface an ambiguous delete rather than swallowing it into
        // `Ok(None)`, which the caller would render as a false "not found".
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("delete".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_delete_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let result = router
            .delete_drawer_remote_with_operation("d-1", Some("op-del-unknown-2"))
            .await
            .unwrap()
            .expect("unknown delete outcome must not be swallowed");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["operation_id"], "op-del-unknown-2");
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.as_slice(), &[Some("op-del-unknown-2".to_owned())]);
    }

    #[tokio::test]
    async fn all_remote_delete_drawer_omitted_operation_id_generates_and_returns_same_id_on_unknown_outcome()
     {
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("delete".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_delete_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let result = router
            .delete_drawer_remote_with_operation("d-1", None)
            .await
            .unwrap()
            .expect("unknown delete outcome must return a structured result");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["success"], false);
        let op_id =
            result["operation_id"].as_str().expect("generated operation_id must be a string");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");

        let seen = received_ids.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[Some(op_id.to_owned())],
            "remote must receive the exact generated operation_id"
        );
    }

    #[tokio::test]
    async fn kg_add_unknown_outcome_returns_structured_result() {
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("kg_add".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_kg_add_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .kg_add_remote_with_operation(
                "Alice",
                "loves",
                "Bob",
                Some("2026-01-01"),
                &route,
                Some("op-kg-unknown-1"),
            )
            .await
            .unwrap()
            .expect("unknown kg_add outcome must return a structured result");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["success"], false);
        assert_eq!(result["remote"], "mock");
        assert_eq!(result["operation_id"], "op-kg-unknown-1");
        assert!(result["retry"].as_str().unwrap().contains("same operation_id"));
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.as_slice(), &[Some("op-kg-unknown-1".to_owned())]);
    }

    #[tokio::test]
    async fn kg_add_omitted_operation_id_generates_and_returns_same_id_on_unknown_outcome() {
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("kg_add".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_kg_add_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .kg_add_remote_with_operation("Alice", "loves", "Bob", Some("2026-01-01"), &route, None)
            .await
            .unwrap()
            .expect("unknown kg_add outcome must return a structured result");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["success"], false);
        assert_eq!(result["remote"], "mock");
        let op_id =
            result["operation_id"].as_str().expect("generated operation_id must be a string");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");
        assert!(result["retry"].as_str().unwrap().contains("same operation_id"));

        let seen = received_ids.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[Some(op_id.to_owned())],
            "remote must receive the exact generated operation_id"
        );
    }

    #[tokio::test]
    async fn kg_invalidate_unknown_outcome_returns_structured_result() {
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("kg_invalidate".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_kg_invalidate_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .kg_invalidate_remote_with_operation(
                "Alice",
                "loves",
                "Bob",
                Some("2026-02-01"),
                &route,
                Some("op-kg-unknown-2"),
            )
            .await
            .unwrap()
            .expect("unknown kg_invalidate outcome must return a structured result");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["success"], false);
        assert_eq!(result["remote"], "mock");
        assert_eq!(result["operation_id"], "op-kg-unknown-2");
        assert!(result["retry"].as_str().unwrap().contains("same operation_id"));
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.as_slice(), &[Some("op-kg-unknown-2".to_owned())]);
    }

    #[tokio::test]
    async fn kg_invalidate_omitted_operation_id_generates_and_returns_same_id_on_unknown_outcome() {
        let mut mock = MockRemote::default();
        mock.fail_unknown_on = Some("kg_invalidate".to_owned());
        let received_ids = std::sync::Arc::clone(&mock.received_kg_invalidate_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .kg_invalidate_remote_with_operation(
                "Alice",
                "loves",
                "Bob",
                Some("2026-02-01"),
                &route,
                None,
            )
            .await
            .unwrap()
            .expect("unknown kg_invalidate outcome must return a structured result");

        assert_eq!(result["outcome"], "unknown_outcome");
        assert_eq!(result["success"], false);
        assert_eq!(result["remote"], "mock");
        let op_id =
            result["operation_id"].as_str().expect("generated operation_id must be a string");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");
        assert!(result["retry"].as_str().unwrap().contains("same operation_id"));

        let seen = received_ids.lock().unwrap();
        assert_eq!(
            seen.as_slice(),
            &[Some(op_id.to_owned())],
            "remote must receive the exact generated operation_id"
        );
    }

    #[tokio::test]
    async fn add_drawer_omitted_operation_id_sends_generated_id_to_remote_on_success() {
        let mock = MockRemote::default();
        let received_ids = std::sync::Arc::clone(&mock.received_add_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote_with_operation(
                "w", "r", "content", "file.txt", "agent", &route, 0.9, None,
            )
            .await
            .unwrap()
            .expect("add must succeed");

        assert_eq!(result["success"], true);
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let op_id = seen[0].as_deref().expect("must send generated operation_id");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");
    }

    #[tokio::test]
    async fn routed_delete_drawer_omitted_operation_id_sends_generated_id_to_remote_on_success() {
        let mock = MockRemote::default();
        let received_ids = std::sync::Arc::clone(&mock.received_delete_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .delete_drawer_routed_remote("d-1", &route, None)
            .await
            .unwrap()
            .expect("delete must succeed");

        assert_eq!(result["success"], true);
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let op_id = seen[0].as_deref().expect("must send generated operation_id");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");
    }

    #[tokio::test]
    async fn all_remote_delete_drawer_omitted_operation_id_sends_generated_id_to_remote_on_success()
    {
        let mock = MockRemote::default();
        let received_ids = std::sync::Arc::clone(&mock.received_delete_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let result = router
            .delete_drawer_remote_with_operation("d-1", None)
            .await
            .unwrap()
            .expect("delete must succeed");

        assert_eq!(result["success"], true);
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let op_id = seen[0].as_deref().expect("must send generated operation_id");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");
    }

    #[tokio::test]
    async fn kg_add_omitted_operation_id_sends_generated_id_to_remote_on_success() {
        let mock = MockRemote::default();
        let received_ids = std::sync::Arc::clone(&mock.received_kg_add_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .kg_add_remote_with_operation("Alice", "loves", "Bob", Some("2026-01-01"), &route, None)
            .await
            .unwrap()
            .expect("kg_add must succeed");

        assert_eq!(result["success"], true);
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let op_id = seen[0].as_deref().expect("must send generated operation_id");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");
    }

    #[tokio::test]
    async fn kg_invalidate_omitted_operation_id_sends_generated_id_to_remote_on_success() {
        let mock = MockRemote::default();
        let received_ids = std::sync::Arc::clone(&mock.received_kg_invalidate_operation_ids);
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .kg_invalidate_remote_with_operation(
                "Alice",
                "loves",
                "Bob",
                Some("2026-02-01"),
                &route,
                None,
            )
            .await
            .unwrap()
            .expect("kg_invalidate must succeed");

        assert_eq!(result["success"], true);
        let seen = received_ids.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let op_id = seen[0].as_deref().expect("must send generated operation_id");
        assert!(!op_id.is_empty(), "generated operation_id must be non-empty");
    }

    #[tokio::test]
    async fn e2e_add_drawer_local_on_local_route() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route =
            ResolvedRouteRule { mode: RouteMode::Local, remote: None, write: WriteTarget::Local };
        let result = router
            .add_drawer_remote("w", "r", "content", "file.txt", "agent", &route, 0.9)
            .await
            .unwrap();

        assert!(result.is_none(), "local route should not produce remote result");
    }

    #[tokio::test]
    async fn e2e_add_drawer_remote_skipped_for_diary_wing() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote(
                SHARED_AGENT_DIARY_WING,
                "r",
                "content",
                "file.txt",
                "agent",
                &route,
                0.9,
            )
            .await
            .unwrap();

        assert!(result.is_none(), "diary wing should not write remotely");
    }

    #[tokio::test]
    async fn e2e_add_drawer_remote_skipped_for_diary_room() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote("wing_code", DIARY_ROOM, "content", "file.txt", "agent", &route, 0.9)
            .await
            .unwrap();

        assert!(result.is_none(), "diary room should not write remotely");
    }

    #[tokio::test]
    async fn e2e_add_drawer_remote_skipped_for_diary_source_file() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote(
                "wing_code",
                "room",
                "content",
                "diary:standup",
                "agent",
                &route,
                0.9,
            )
            .await
            .unwrap();

        assert!(result.is_none(), "diary source_file should not write remotely");
    }

    #[tokio::test]
    async fn e2e_check_duplicate_fans_out() {
        let mut mock = MockRemote::default();
        mock.duplicate_matches = vec![json!({"drawer_id":"dup-1","similarity":0.95})];
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let results = router.check_duplicate_all_remotes("content", 0.9).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["drawer_id"], "dup-1");
        assert_eq!(results[0]["origin"], "alpha");
    }

    #[tokio::test]
    async fn e2e_delete_drawer_tries_all_remotes_in_order() {
        // First remote fails (non-degradable 404), second succeeds.
        let mut mock_fail = MockRemote::default();
        mock_fail.delete_succeeds = false;
        let mock_ok = MockRemote::default();

        let mut remotes = BTreeMap::new();
        remotes.insert("aaa".to_owned(), Arc::new(mock_fail) as Arc<dyn RemoteApi>);
        remotes.insert("bbb".to_owned(), Arc::new(mock_ok) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let result = router.delete_drawer_remote("drawer-1").await.unwrap().unwrap();

        // BTreeMap order: "aaa" before "bbb"; aaa fails, bbb succeeds
        assert_eq!(result["success"], true);
        assert_eq!(result["drawer_id"], "drawer-1");
        assert_eq!(result["origin"], "bbb");
        assert_eq!(result["applied_to"], "remote:bbb");
    }

    #[tokio::test]
    async fn e2e_delete_drawer_remote_succeeds() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let result = router.delete_drawer_remote("drawer-1").await.unwrap().unwrap();

        assert_eq!(result["success"], true);
        assert_eq!(result["drawer_id"], "drawer-1");
        assert_eq!(result["origin"], "alpha");
        assert_eq!(result["applied_to"], "remote:alpha");
    }

    #[tokio::test]
    async fn e2e_delete_drawer_no_remotes_returns_none() {
        let router = FederationRouter::new(FederationRuntimeConfig::default());
        let result = router.delete_drawer_remote("drawer-1").await.unwrap();
        assert!(result.is_none());
    }

    // ── DeleteDrawer route-matrix: verify exclusion from write routing ──────
    //
    // These tests are deliberately in federation.rs (the low-level module) and
    // verify that `delete_drawer_remote` — the method called by
    // `tool_delete_drawer` as a fallback — ignores routing rules and tries all
    // remotes.  The full local-first + fallback path is covered in lib.rs tests
    // via `tool_delete_drawer` directly.

    #[tokio::test]
    async fn e2e_delete_drawer_remote_ignores_routing() {
        // Configure a Combined/write:Both route.  delete_drawer_remote must
        // still try remotes by its own logic (iterate all in order) rather than
        // delegating to a replicate path.  This test proves the low-level
        // function is agnostic to routing.
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let mut router = make_router(remotes);
        router.rules.wings.insert(
            "wing_code".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Combined,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Both,
            },
        );

        let result = router.delete_drawer_remote("drawer-1").await.unwrap().unwrap();
        assert_eq!(result["success"], true);
        assert_eq!(result["origin"], "alpha");
    }

    #[tokio::test]
    async fn e2e_status_merge_populates_url_and_info() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = json!({"wings":{"wing_code":5},"rooms":{},"drawers_total":5});
        let result = router.status_merge(local).await.unwrap();

        let fed = &result["federation"]["remotes"][0];
        assert_eq!(fed["name"], "alpha");
        assert_eq!(fed["url"], "https://test.example");
        assert_eq!(fed["reachable"], true);
        assert_eq!(fed["federation_api_version"], 1);
    }

    #[tokio::test]
    async fn e2e_taxonomy_merge_unions_wing_counts() {
        let mut mock = MockRemote::default();
        mock.taxonomy = json!({"taxonomy":{"wing_code":{"room_a":2,"room_b":3}}});
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = json!({"taxonomy":{"wing_code":{"room_a":1,"room_c":4}}});
        let result = router.taxonomy_merge(local).await.unwrap();

        let taxonomy = &result["taxonomy"]["wing_code"];
        assert_eq!(taxonomy["room_a"], 3); // 1 + 2
        assert_eq!(taxonomy["room_b"], 3); // 0 + 3
        assert_eq!(taxonomy["room_c"], 4); // 4 + 0
    }

    #[tokio::test]
    async fn e2e_kg_query_merges_facts() {
        let mut mock = MockRemote::default();
        mock.kg_query_response = json!({
            "entity": "Alice",
            "facts": [
                {"subject":"A","predicate":"knows","object":"D","valid_from":"2026-02-01","direction":"outgoing"},
            ],
            "count": 1
        });
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = json!({
            "entity": "Alice",
            "facts": [
                {"subject":"A","predicate":"loves","object":"B","valid_from":"2026-01-01","direction":"outgoing"},
            ],
            "count": 1,
            "as_of": null,
        });
        let route = ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("alpha".to_owned()),
            write: WriteTarget::Remote,
        };
        let result = router.kg_query_merge(local, "Alice", &route).await.unwrap();

        assert_eq!(result["count"], 2);
        let facts = result["facts"].as_array().unwrap();
        assert_eq!(facts.len(), 2);
        // Remote fact should have origin annotation
        let remote_fact = facts.iter().find(|f| f["origin"].as_str() == Some("alpha"));
        assert!(remote_fact.is_some());
    }

    #[tokio::test]
    async fn e2e_kg_timeline_merges_local_and_remote() {
        let mut mock = MockRemote::default();
        mock.kg_timeline_response = json!({
            "entity": "all",
            "timeline": [
                {"subject":"A","predicate":"knows","object":"C","valid_from":"2026-03-01","current":true},
            ],
            "count": 1
        });
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = json!({
            "entity": "all",
            "timeline": [
                {"subject":"A","predicate":"loves","object":"B","valid_from":"2026-01-01","current":true},
            ],
            "count": 1,
        });
        let route = ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("alpha".to_owned()),
            write: WriteTarget::Remote,
        };
        let result = router.kg_timeline_merge(local, None, &route).await.unwrap();

        assert_eq!(result["count"], 2);
        let timeline = result["timeline"].as_array().unwrap();
        assert_eq!(timeline.len(), 2);
    }

    #[tokio::test]
    async fn e2e_kg_stats_merges_numerics() {
        let mut mock = MockRemote::default();
        mock.kg_stats_response = json!({
            "entities": 7,
            "triples": 15,
            "current_facts": 10,
            "expired_facts": 5,
            "relationship_types": ["knows", "works_on"],
        });
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = json!({
            "entities": 10,
            "triples": 25,
            "current_facts": 20,
            "expired_facts": 5,
            "relationship_types": ["loves", "works_on"],
        });
        let route = ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("alpha".to_owned()),
            write: WriteTarget::Remote,
        };
        let result = router.kg_stats_merge(local, &route).await.unwrap();

        assert_eq!(result["entities"], 17);
        assert_eq!(result["triples"], 40);
        assert_eq!(result["current_facts"], 30);
        assert_eq!(result["expired_facts"], 10);
    }

    #[tokio::test]
    async fn e2e_kg_query_degradable_on_remote_outage_adds_warning() {
        let mut mock = MockRemote::default();
        mock.fail_on = Some("kg_query".to_owned());
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = json!({
            "entity": "Alice",
            "facts": [{"subject":"A","predicate":"loves","object":"B","valid_from":"2026-01-01","direction":"outgoing"}],
            "count": 1,
            "as_of": null,
        });
        let route = ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("alpha".to_owned()),
            write: WriteTarget::Remote,
        };
        let result = router.kg_query_merge(local, "Alice", &route).await.unwrap();

        // Should return local results with a warning when remote fails
        assert_eq!(result["count"], 1);
        let facts = result["facts"].as_array().unwrap();
        assert_eq!(facts.len(), 1);
        let warnings = result["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].as_str().unwrap().contains("alpha"));
        // Structured degradation accompanies the legacy string warning.
        let degradations = result["degradations"].as_array().unwrap();
        assert_eq!(degradations.len(), 1);
        assert_eq!(degradations[0]["code"], "remote_read_degraded");
        assert_eq!(degradations[0]["remote"], "alpha");
        assert_eq!(degradations[0]["kind"], "kg_query");
        assert_eq!(degradations[0]["classification"], "unreachable");
    }

    #[tokio::test]
    async fn e2e_kg_add_returns_applied_to_remote() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = ResolvedRouteRule {
            mode: RouteMode::Remote,
            remote: Some("alpha".to_owned()),
            write: WriteTarget::Remote,
        };
        let result = router.kg_add_remote("A", "loves", "B", None, &route).await.unwrap().unwrap();
        assert_eq!(result["applied_to"], "remote:alpha");
    }

    #[tokio::test]
    async fn e2e_kg_invalidate_returns_applied_to_remote() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = ResolvedRouteRule {
            mode: RouteMode::Remote,
            remote: Some("alpha".to_owned()),
            write: WriteTarget::Remote,
        };
        let result =
            router.kg_invalidate_remote("A", "loves", "B", None, &route).await.unwrap().unwrap();
        assert_eq!(result["applied_to"], "remote:alpha");
    }

    // ── KG replicate tests ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn e2e_kg_add_remote_skips_work_for_both() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let result = router.kg_add_remote("A", "loves", "B", None, &route).await.unwrap();

        assert!(result.is_none(), "Both route should not produce remote result from kg_add_remote");
    }

    #[tokio::test]
    async fn e2e_kg_add_replicate_succeeds() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router.kg_add_replicate("A", "loves", "B", None, &route).await;

        assert_eq!(status, ReplicationStatus::Replicated { remote: "alpha".to_owned() });
    }

    #[tokio::test]
    async fn e2e_kg_add_replicate_remote_failure() {
        let mut mock = MockRemote::default();
        mock.fail_on = Some("kg_add".to_owned());
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router.kg_add_replicate("A", "loves", "B", None, &route).await;

        match status {
            ReplicationStatus::Failed { remote, .. } => {
                assert_eq!(remote, "alpha");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn e2e_kg_add_replicate_skipped_for_non_both() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let status = router.kg_add_replicate("A", "loves", "B", None, &route).await;

        assert_eq!(status, ReplicationStatus::Skipped);
    }

    #[tokio::test]
    async fn e2e_kg_invalidate_remote_skips_work_for_both() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let result = router.kg_invalidate_remote("A", "loves", "B", None, &route).await.unwrap();

        assert!(
            result.is_none(),
            "Both route should not produce remote result from kg_invalidate_remote"
        );
    }

    #[tokio::test]
    async fn e2e_kg_invalidate_replicate_succeeds() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router.kg_invalidate_replicate("A", "loves", "B", None, &route).await;

        assert_eq!(status, ReplicationStatus::Replicated { remote: "alpha".to_owned() });
    }

    #[tokio::test]
    async fn e2e_kg_invalidate_replicate_remote_failure() {
        let mut mock = MockRemote::default();
        mock.fail_on = Some("kg_invalidate".to_owned());
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router.kg_invalidate_replicate("A", "loves", "B", None, &route).await;

        match status {
            ReplicationStatus::Failed { remote, .. } => {
                assert_eq!(remote, "alpha");
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn e2e_kg_invalidate_replicate_skipped_for_non_both() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let status = router.kg_invalidate_replicate("A", "loves", "B", None, &route).await;

        assert_eq!(status, ReplicationStatus::Skipped);
    }

    #[tokio::test]
    async fn e2e_wing_availability_includes_configured_wings() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        // Simulate local wings; default_mode=Combined → "combined"
        let mut local_wings = BTreeMap::new();
        local_wings.insert("wing_code".to_owned(), 5);

        let avail = router.wing_availability(&local_wings);
        assert_eq!(avail["wing_code"], "combined");
    }

    #[tokio::test]
    async fn e2e_wing_availability_with_explicit_wing_rule() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);

        let remotes_config = {
            let mut m = BTreeMap::new();
            m.insert("alpha".to_owned(), make_resolved_remote("alpha"));
            m
        };
        let mut wings_config = BTreeMap::new();
        wings_config.insert(
            "wing_external".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Local,
            },
        );
        let rules = FederationRuntimeConfig {
            remotes: remotes_config,
            default_mode: RouteMode::Combined,
            default_remote: Some("alpha".to_owned()),
            wings: wings_config,
            kg: None,
            coordination: BTreeMap::new(),
        };
        let router = FederationRouter::with_remotes(rules, remotes);

        // wing_external is configured but not local → should appear as "remote:alpha"
        let local_wings = BTreeMap::new();
        let avail = router.wing_availability(&local_wings);
        assert!(avail.get("wing_external").is_some());
        // The wing_external rule has mode=Remote → "remote:alpha"
        assert_eq!(avail["wing_external"], "remote:alpha");
    }

    #[tokio::test]
    async fn e2e_kg_timeline_degradable_adds_warning() {
        let mut mock = MockRemote::default();
        mock.fail_on = Some("kg_timeline".to_owned());
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = json!({"entity":"all","timeline":[],"count":0});
        let route = ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("alpha".to_owned()),
            write: WriteTarget::Remote,
        };
        let result = router.kg_timeline_merge(local, None, &route).await.unwrap();
        let warnings = result["warnings"].as_array().unwrap();
        assert!(warnings[0].as_str().unwrap().contains("alpha"));
        let degradations = result["degradations"].as_array().unwrap();
        assert_eq!(degradations[0]["kind"], "kg_timeline");
        assert_eq!(degradations[0]["remote"], "alpha");
        assert_eq!(degradations[0]["classification"], "unreachable");
    }

    #[tokio::test]
    async fn e2e_kg_stats_degradable_adds_warning() {
        let mut mock = MockRemote::default();
        mock.fail_on = Some("kg_stats".to_owned());
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = json!({"entities":5,"triples":10,"current_facts":8,"expired_facts":2,"relationship_types":["loves"]});
        let route = ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("alpha".to_owned()),
            write: WriteTarget::Remote,
        };
        let result = router.kg_stats_merge(local, &route).await.unwrap();
        assert_eq!(result["entities"], 5);
        let warnings = result["warnings"].as_array().unwrap();
        assert!(warnings[0].as_str().unwrap().contains("alpha"));
        let degradations = result["degradations"].as_array().unwrap();
        assert_eq!(degradations[0]["kind"], "kg_stats");
        assert_eq!(degradations[0]["remote"], "alpha");
        assert_eq!(degradations[0]["classification"], "unreachable");
    }

    // ─── plan_search_targets unit tests ─────────────────────────────────────

    fn make_router_with_rules(
        remotes: BTreeMap<String, Arc<dyn RemoteApi>>,
        wings: BTreeMap<String, ResolvedRouteRule>,
        default_mode: RouteMode,
        default_remote: Option<String>,
    ) -> FederationRouter {
        let mut rules_remotes = BTreeMap::new();
        for name in remotes.keys() {
            rules_remotes.insert(name.clone(), make_resolved_remote(name));
        }
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode,
            default_remote,
            wings,
            kg: None,
            coordination: BTreeMap::new(),
        };
        FederationRouter::with_remotes(rules, remotes)
    }

    fn mock_remote_arc() -> Arc<dyn RemoteApi> {
        Arc::new(MockRemote::default()) as Arc<dyn RemoteApi>
    }

    #[test]
    fn plan_search_diary_wing_guard_wins_over_remote_wing_rule() {
        // Even if the diary wing has a Remote rule, plan_search_targets returns (true, []).
        let mut wings = BTreeMap::new();
        wings.insert(
            SHARED_AGENT_DIARY_WING.to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Local,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), mock_remote_arc());
        let router = make_router_with_rules(remotes, wings, RouteMode::Local, None);
        let (include_local, targets) =
            router.plan_search_targets(Some(SHARED_AGENT_DIARY_WING), None);
        assert!(include_local);
        assert!(targets.is_empty());
    }

    #[test]
    fn plan_search_diary_room_guard() {
        // room == DIARY_ROOM triggers the guard regardless of wing.
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), mock_remote_arc());
        let router = make_router_with_rules(
            remotes,
            BTreeMap::new(),
            RouteMode::Combined,
            Some("alpha".to_owned()),
        );
        let (include_local, targets) =
            router.plan_search_targets(Some("wing_code"), Some(DIARY_ROOM));
        assert!(include_local);
        assert!(targets.is_empty());
    }

    #[test]
    fn plan_search_explicit_wing_remote() {
        let mut wings = BTreeMap::new();
        wings.insert(
            "wing_ext".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Local,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), mock_remote_arc());
        let router = make_router_with_rules(remotes, wings, RouteMode::Local, None);
        let (include_local, targets) = router.plan_search_targets(Some("wing_ext"), None);
        assert!(!include_local);
        assert_eq!(targets, vec!["alpha".to_owned()]);
    }

    #[test]
    fn plan_search_explicit_wing_combined() {
        let mut wings = BTreeMap::new();
        wings.insert(
            "wing_combo".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Combined,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Remote,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), mock_remote_arc());
        let router = make_router_with_rules(remotes, wings, RouteMode::Local, None);
        let (include_local, targets) = router.plan_search_targets(Some("wing_combo"), None);
        assert!(include_local);
        assert_eq!(targets, vec!["alpha".to_owned()]);
    }

    #[test]
    fn plan_search_explicit_wing_local() {
        let mut wings = BTreeMap::new();
        wings.insert(
            "wing_local".to_owned(),
            ResolvedRouteRule { mode: RouteMode::Local, remote: None, write: WriteTarget::Local },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), mock_remote_arc());
        let router =
            make_router_with_rules(remotes, wings, RouteMode::Combined, Some("alpha".to_owned()));
        let (include_local, targets) = router.plan_search_targets(Some("wing_local"), None);
        assert!(include_local);
        assert!(targets.is_empty());
    }

    #[test]
    fn plan_search_no_wing_default_local_two_wing_rules_different_remotes() {
        // wing=None, default_mode=Local, two wing rules pointing at two different remotes.
        // Both remotes should appear in targets.
        let mut wings = BTreeMap::new();
        wings.insert(
            "wing_a".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Local,
            },
        );
        wings.insert(
            "wing_b".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("beta".to_owned()),
                write: WriteTarget::Local,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), mock_remote_arc());
        remotes.insert("beta".to_owned(), mock_remote_arc());
        let router = make_router_with_rules(remotes, wings, RouteMode::Local, None);
        let (include_local, mut targets) = router.plan_search_targets(None, None);
        assert!(include_local, "global search always includes local");
        targets.sort();
        assert_eq!(targets, vec!["alpha".to_owned(), "beta".to_owned()]);
    }

    #[test]
    fn plan_search_no_wing_default_combined_includes_default_remote() {
        // wing=None, default_mode=Combined → default remote is included.
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), mock_remote_arc());
        let router = make_router_with_rules(
            remotes,
            BTreeMap::new(),
            RouteMode::Combined,
            Some("alpha".to_owned()),
        );
        let (include_local, targets) = router.plan_search_targets(None, None);
        assert!(include_local);
        assert_eq!(targets, vec!["alpha".to_owned()]);
    }

    #[test]
    fn plan_search_targets_filtered_to_built_remotes() {
        // A wing rule names "ghost" which was never built into self.remotes
        // (e.g. client construction failed). Should be excluded from targets.
        let mut wings = BTreeMap::new();
        wings.insert(
            "wing_a".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("ghost".to_owned()),
                write: WriteTarget::Local,
            },
        );
        wings.insert(
            "wing_b".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Remote,
                remote: Some("alpha".to_owned()),
                write: WriteTarget::Local,
            },
        );
        // Only "alpha" is in remotes, not "ghost".
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), mock_remote_arc());
        let router = make_router_with_rules(remotes, wings, RouteMode::Local, None);
        let (include_local, targets) = router.plan_search_targets(None, None);
        assert!(include_local);
        assert_eq!(targets, vec!["alpha".to_owned()]);
    }

    // ─── Dedup cross-side test ───────────────────────────────────────────────

    #[test]
    fn dedup_local_no_hash_and_remote_with_hash_same_text() {
        // Local item has no content_hash; remote item has a hash but same text.
        // The text-based dedup should catch the duplicate.
        let local = vec![json!({"text": "shared content", "wing": "w", "room": "r"})];
        let remote = vec![json!({
            "text": "shared content",
            "wing": "w",
            "room": "r2",
            "content_hash": "abc123"
        })];
        let origins = vec![("local".to_owned(), local), ("alpha".to_owned(), remote)];
        let merged = merge_search_results_nway(origins, 10);
        assert_eq!(merged.len(), 1, "cross-side duplicate should be deduped to 1");
        // Local item is preferred (it was inserted first).
        assert!(merged[0].get("content_hash").is_none());
    }

    // ─── changes_fanout unit tests ────────────────────────────────────────────

    fn make_change_event(
        event_type: &str,
        occurred_at: &str,
        entity_id: &str,
    ) -> mempalace_federation::ChangeEventDto {
        mempalace_federation::ChangeEventDto {
            event_type: event_type.to_owned(),
            occurred_at: occurred_at.to_owned(),
            entity_id: entity_id.to_owned(),
            actor: None,
            details: None,
        }
    }

    #[tokio::test]
    async fn changes_fanout_annotates_origin() {
        let mut mock = MockRemote::default();
        mock.changes_events =
            vec![make_change_event("drawer_added", "2026-06-10T10:00:00Z", "entity-1")];
        let mut remotes = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let cursors = BTreeMap::new();
        let results = router.changes_fanout(None, None, &cursors).await;

        assert!(results.contains_key("hub"), "expected hub in results");
        let hub = &results["hub"];
        let events = hub["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["origin"], "remote:hub");
        assert_eq!(events[0]["event_type"], "drawer_added");
        assert_eq!(hub["next_cursor"], Value::Null);
    }

    #[tokio::test]
    async fn changes_fanout_passes_cursor_to_correct_remote() {
        let cursor_store = std::sync::Arc::new(std::sync::Mutex::new(None));
        let mut mock = MockRemote::default();
        mock.received_cursor = std::sync::Arc::clone(&cursor_store);

        let mut remotes = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let mut cursors = BTreeMap::new();
        cursors.insert("hub".to_owned(), "tok-abc123".to_owned());

        let _ = router.changes_fanout(None, None, &cursors).await;

        let received = cursor_store.lock().unwrap().clone();
        assert_eq!(received.as_deref(), Some("tok-abc123"));
    }

    #[tokio::test]
    async fn changes_fanout_unreachable_remote_is_marked_and_does_not_poison_healthy() {
        let mut failing = MockRemote::default();
        failing.fail_on = Some("changes".to_owned());

        let mut healthy = MockRemote::default();
        healthy.changes_events =
            vec![make_change_event("drawer_added", "2026-06-10T11:00:00Z", "entity-ok")];

        let mut remotes = BTreeMap::new();
        remotes.insert("down".to_owned(), Arc::new(failing) as Arc<dyn RemoteApi>);
        remotes.insert("ok".to_owned(), Arc::new(healthy) as Arc<dyn RemoteApi>);

        let rules_remotes = {
            let mut m = BTreeMap::new();
            m.insert("down".to_owned(), make_resolved_remote("down"));
            m.insert("ok".to_owned(), make_resolved_remote("ok"));
            m
        };
        let rules = FederationRuntimeConfig {
            remotes: rules_remotes,
            default_mode: RouteMode::Combined,
            default_remote: Some("ok".to_owned()),
            wings: BTreeMap::new(),
            kg: None,
            coordination: BTreeMap::new(),
        };
        let router = FederationRouter::with_remotes(rules, remotes);

        let cursors = BTreeMap::new();
        let results = router.changes_fanout(None, None, &cursors).await;

        // "down" → unreachable marker
        let down = &results["down"];
        assert_eq!(down["unreachable"], true, "down should be unreachable");
        assert!(down["error"].as_str().map_or(false, |e| !e.is_empty()));

        // "ok" → healthy events with origin annotation
        let ok = &results["ok"];
        let events = ok["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["origin"], "remote:ok");
    }

    #[tokio::test]
    async fn changes_fanout_surfaces_next_cursor() {
        let mut mock = MockRemote::default();
        mock.changes_next_cursor = Some("cursor-next-42".to_owned());

        let mut remotes = BTreeMap::new();
        remotes.insert("hub".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let cursors = BTreeMap::new();
        let results = router.changes_fanout(None, None, &cursors).await;

        assert_eq!(results["hub"]["next_cursor"], "cursor-next-42");
    }
}
