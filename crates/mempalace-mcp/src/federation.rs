use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use mempalace_config::{
    FederationRuntimeConfig, ReplicationStatus, ResolvedRouteRule, RouteMode, WriteTarget,
    resolve_kg_route, resolve_route, RouteQuery,
};
use mempalace_core::{hash_text, DIARY_ROOM, DIARY_TOPIC_PREFIX, SHARED_AGENT_DIARY_WING};
use mempalace_federation::{
    AddDrawerRequest, ChangesQuery, DrawerSearchRequest, RemoteDrawerResult,
};
use mempalace_remote::{
    RemoteApi, RemoteClient, RemoteEndpoint, RemoteError,
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
            return Ok(search_payload(query, wing, room, local_results, &[]));
        }

        // Fan out to all target remotes concurrently.
        let mut set: JoinSet<(String, Result<Vec<Value>, String>)> = JoinSet::new();
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
                    Err(e) => (name.clone(), Err(format!("remote `{name}` search failed: {e}"))),
                }
            });
        }

        // Collect results in deterministic name order.
        let mut remote_results_by_name: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        let mut warnings: Vec<String> = Vec::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(results))) => {
                    remote_results_by_name.insert(name, results);
                }
                Ok((name, Err(msg))) => {
                    tracing::warn!(remote = %name, "search degraded: {msg}");
                    warnings.push(msg);
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
        Ok(search_payload(query, wing, room, merged, &warnings))
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
            }
        };
        let Some(remote_name) = target_remote else {
            return Ok(None);
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(None);
        };

        // ── Pre-add duplicate check ───────────────────────────────────────────
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

        let req = AddDrawerRequest {
            wing: wing.to_owned(),
            room: room.to_owned(),
            content: content.to_owned(),
            source_file: if source_file.is_empty() { None } else { Some(source_file.to_owned()) },
            added_by: Some(added_by.to_owned()),
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
            Err(RemoteError::RemoteRejected { status: 409, .. }) => {
                // Race condition: duplicate inserted between pre-check and add.
                Ok(Some(json!({
                    "success": false,
                    "reason": "duplicate",
                    "matches": [],
                    "origin": remote_name,
                    "applied_to": format_remote_origin(remote_name),
                })))
            }
            Err(e) => {
                Err(ToolError::Internal(McpError::Federation(format!(
                    "remote `{remote_name}` add_drawer failed: {e}"
                ))))
            }
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
            Err(RemoteError::RemoteRejected { status: 409, .. }) => {
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
    pub async fn check_duplicate_all_remotes(
        &self,
        content: &str,
        threshold: f32,
    ) -> Vec<Value> {
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

    /// Try to delete a drawer from ALL configured remotes in config order (BTreeMap
    /// name order — deterministic). Returns the first success with "origin".
    /// Deletion is by drawer id; the wing is not known here.
    pub async fn delete_drawer_remote(
        &self,
        drawer_id: &str,
    ) -> ToolResult<Option<Value>> {
        for (name, api) in &self.remotes {
            match api.delete_drawer(drawer_id).await {
                Ok(()) => {
                    return Ok(Some(json!({
                        "success": true,
                        "drawer_id": drawer_id,
                        "origin": name,
                        "applied_to": format_remote_origin(name),
                    })));
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
    pub async fn taxonomy_merge(
        &self,
        local_taxonomy: Value,
    ) -> ToolResult<Value> {
        let mut set: JoinSet<(String, Result<Value, mempalace_remote::RemoteError>)> = JoinSet::new();
        for (name, api) in &self.remotes {
            let name = name.clone();
            let api = Arc::clone(api);
            set.spawn(async move { (name, api.taxonomy().await) });
        }

        let mut remote_results: BTreeMap<String, Value> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(payload))) => { remote_results.insert(name, payload); }
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
                if let (Some(obj), Some(robj)) =
                    (merged.get_mut("taxonomy").and_then(|v| v.as_object_mut()), remote_taxonomy.as_object())
                {
                    for (wing, rooms) in robj {
                        if let Some(rooms_obj) = rooms.as_object() {
                            let wing_entry =
                                obj.entry(wing.clone()).or_insert_with(|| json!({}));
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

    pub async fn wings_merge(
        &self,
        local_wings: Value,
    ) -> ToolResult<Value> {
        let mut set: JoinSet<(String, Result<Value, mempalace_remote::RemoteError>)> = JoinSet::new();
        for (name, api) in &self.remotes {
            let name = name.clone();
            let api = Arc::clone(api);
            set.spawn(async move { (name, api.wings().await) });
        }

        let mut remote_results: BTreeMap<String, Value> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(payload))) => { remote_results.insert(name, payload); }
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
                if let (Some(obj), Some(robj)) =
                    (merged.get_mut("wings").and_then(|v| v.as_object_mut()), remote_wings.as_object())
                {
                    for (wing, count) in robj {
                        // Wing-collision warning: wing exists locally AND on remote
                        // but the resolved route is Local-only.
                        if local_wing_names.contains(wing) {
                            let rule = resolve_route(
                                &self.rules,
                                None,
                                RouteQuery { wing: Some(wing.as_str()), room: None, source_file: None },
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
        let mut set: JoinSet<(String, Result<Value, mempalace_remote::RemoteError>)> = JoinSet::new();
        for (name, api) in &self.remotes {
            let name = name.clone();
            let api = Arc::clone(api);
            let wf = wing_filter_owned.clone();
            set.spawn(async move { (name, api.rooms(wf.as_deref()).await) });
        }

        let mut remote_results: BTreeMap<String, Value> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, Ok(payload))) => { remote_results.insert(name, payload); }
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
                if let (Some(obj), Some(robj)) =
                    (merged.get_mut("rooms").and_then(|v| v.as_object_mut()), remote_rooms.as_object())
                {
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

    pub async fn status_merge(
        &self,
        mut local_status: Value,
    ) -> ToolResult<Value> {
        let mut set: JoinSet<(String, String, Result<mempalace_federation::InfoResponse, mempalace_remote::RemoteError>)> = JoinSet::new();
        for (name, api) in &self.remotes {
            let name = name.clone();
            let url = self
                .rules
                .remotes
                .get(&name)
                .map(|r| r.url.clone())
                .unwrap_or_default();
            let api = Arc::clone(api);
            set.spawn(async move { (name, url, api.info().await) });
        }

        let mut info_results: BTreeMap<String, (String, Result<mempalace_federation::InfoResponse, mempalace_remote::RemoteError>)> = BTreeMap::new();
        while let Some(res) = set.join_next().await {
            match res {
                Ok((name, url, result)) => { info_results.insert(name, (url, result)); }
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
            let query = ChangesQuery {
                since: since.clone(),
                limit,
                cursor: cursors.get(&name).cloned(),
            };
            set.spawn(async move {
                match api.changes(query).await {
                    Ok(resp) => {
                        let origin = format_remote_origin(&name);
                        let events: Vec<Value> = resp
                            .events
                            .into_iter()
                            .map(|evt| {
                                let mut v = serde_json::to_value(&evt)
                                    .unwrap_or_else(|_| json!({}));
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
                    results.insert(
                        name,
                        json!({ "unreachable": true, "error": msg }),
                    );
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
            RouteMode::Remote | RouteMode::Combined => {
                route.remote.as_deref().unwrap_or("remote")
            }
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
                let mut payload = local_payload;
                let warn_msg = format!("remote `{remote_name}` kg_query failed: {e}");
                payload["warnings"] = json!([warn_msg]);
                return Ok(payload);
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
                Ok(merge_kg_facts(local_payload, remote_payload, remote_name))
            }
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
            }
        };
        let Some(remote_name) = target_remote else {
            return Ok(None);
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(None);
        };
        let req = mempalace_federation::KgAddFactRequest {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: object.to_owned(),
            valid_from: valid_from.map(|s| s.to_owned()),
        };
        match api.kg_add_fact(req).await {
            Ok(mut resp) => {
                if let Some(obj) = resp.as_object_mut() {
                    obj.insert("applied_to".to_owned(), json!(format_remote_origin(remote_name)));
                }
                Ok(Some(resp))
            }
            Err(e) => {
                Err(ToolError::Internal(McpError::Federation(format!(
                    "remote `{remote_name}` kg_add_fact failed: {e}"
                ))))
            }
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
                ReplicationStatus::Failed {
                    remote: remote_name.to_owned(),
                    reason: format!("{e}"),
                }
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
            }
        };
        let Some(remote_name) = target_remote else {
            return Ok(None);
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(None);
        };
        let req = mempalace_federation::KgInvalidateRequest {
            subject: subject.to_owned(),
            predicate: predicate.to_owned(),
            object: object.to_owned(),
            ended: ended.map(|s| s.to_owned()),
        };
        match api.kg_invalidate(req).await {
            Ok(mut resp) => {
                if let Some(obj) = resp.as_object_mut() {
                    obj.insert("applied_to".to_owned(), json!(format_remote_origin(remote_name)));
                }
                Ok(Some(resp))
            }
            Err(e) => {
                Err(ToolError::Internal(McpError::Federation(format!(
                    "remote `{remote_name}` kg_invalidate failed: {e}"
                ))))
            }
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
                ReplicationStatus::Failed {
                    remote: remote_name.to_owned(),
                    reason: format!("{e}"),
                }
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
            RouteMode::Remote | RouteMode::Combined => {
                route.remote.as_deref().unwrap_or("remote")
            }
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
                let mut payload = local_payload;
                let warn_msg = format!("remote `{remote_name}` kg_timeline failed: {e}");
                payload["warnings"] = json!([warn_msg]);
                return Ok(payload);
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
            RouteMode::Remote | RouteMode::Combined => {
                route.remote.as_deref().unwrap_or("remote")
            }
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
                let mut payload = local_payload;
                let warn_msg = format!("remote `{remote_name}` kg_stats failed: {e}");
                payload["warnings"] = json!([warn_msg]);
                return Ok(payload);
            }
        };
        match route.mode {
            RouteMode::Local => unreachable!(),
            RouteMode::Remote => Ok(remote_payload),
            RouteMode::Combined => Ok(merge_kg_stats(local_payload, remote_payload)),
        }
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
    if name.starts_with("remote:") {
        name.to_owned()
    } else {
        format!("remote:{name}")
    }
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
        obj["relationship_types"] =
            json!(types_set.into_iter().collect::<Vec<_>>());
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
    payload
}

/// N-way rank interleave across origins. `origins` is a list of (origin_name,
/// results) pairs. Local (if present) is expected to be first in the list.
/// Deduplication prefers the first seen (local preferred since it's first).
/// Truncates to `limit`.
pub(crate) fn merge_search_results_nway(
    origins: Vec<(String, Vec<Value>)>,
    limit: usize,
) -> Vec<Value> {
    if origins.is_empty() {
        return vec![];
    }
    if origins.len() == 1 {
        let (origin_name, results) = origins.into_iter().next().unwrap();
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

    let mut merged: Vec<Value> = Vec::with_capacity(limit);
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
                if !is_duplicate_search_item(item, &mut seen_hashes, &mut seen_texts) {
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
    seen_hashes: &mut std::collections::HashSet<String>,
    seen_texts: &mut std::collections::HashSet<String>,
) -> bool {
    // An item is a duplicate if EITHER its hash (if non-empty) is already seen
    // OR its text is already seen. This prevents a local item (no hash) and a
    // remote item (with hash, same text) from both appearing.
    let hash = item["content_hash"].as_str().filter(|h| !h.is_empty());
    let text = item["text"].as_str();

    let hash_dup = hash.map(|h| seen_hashes.contains(h)).unwrap_or(false);
    let text_dup = text.map(|t| seen_texts.contains(t)).unwrap_or(false);

    if hash_dup || text_dup {
        return true;
    }

    // Not a duplicate — register both hash and text for future items.
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
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::collections::BTreeMap;

    use mempalace_config::ResolvedRemote;
    use mempalace_federation::{
        AddDrawerRequest, AddDrawerResponse, ChangesQuery, ChangesResponse,
        CheckDuplicateRequest, CheckDuplicateResponse, DrawerSearchRequest, DrawerSearchResponse,
        InfoResponse, KgAddFactRequest, KgInvalidateRequest, KgQueryRequest,
        ListDrawersQuery, ListDrawersResponse,
    };
    use mempalace_remote::RemoteError;

    // ─── merge_search_results_nway unit tests ────────────────────────────────

    fn local_origins(items: Vec<Value>) -> Vec<(String, Vec<Value>)> {
        vec![("local".to_owned(), items)]
    }

    fn two_origins(local: Vec<Value>, remote: Vec<Value>) -> Vec<(String, Vec<Value>)> {
        let mut v = vec![];
        if !local.is_empty() { v.push(("local".to_owned(), local)); }
        if !remote.is_empty() { v.push(("alpha".to_owned(), remote)); }
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
        let local = vec![
            json!({"wing":"w","room":"r1","similarity":0.9,"text":"same content"}),
        ];
        let remote = vec![
            json!({"wing":"w","room":"r1","similarity":0.85,"text":"same content"}),
        ];
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
        let local =
            vec![json!({"wing":"w","room":"r1","similarity":0.9,"text":"only local"})];
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
        let merged = merge_search_results_nway(
            vec![("alpha".to_owned(), remote)],
            2
        );
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn router_with_no_remotes_has_no_remotes() {
        let router = FederationRouter::new(FederationRuntimeConfig::default());
        assert!(!router.has_remotes());
    }

    #[test]
    fn merge_dedupes_on_content_hash() {
        let local = vec![json!({"wing":"w","room":"r1","similarity":0.9,"text":"hello","content_hash":"abc123"})];
        let remote = vec![json!({"wing":"w","room":"r2","similarity":0.8,"text":"hello","content_hash":"abc123"})];
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
        };
        let router_obj = FederationRouter::new(rules);
        let mut wings = BTreeMap::new();
        wings.insert("wing_code".to_owned(), 5);
        let avail = router_obj.wing_availability(&wings);
        assert_eq!(avail["wing_code"], "combined");
    }

    // ─── E2E federation tests with mock remote ──────────────────────────────

    struct MockRemote {
        info_response: Value,
        search_results: Vec<Value>,
        add_drawer_success: bool,
        add_drawer_409: bool,
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
        changes_events: Vec<mempalace_federation::ChangeEventDto>,
        changes_next_cursor: Option<String>,
        /// When set, the `changes` call records the incoming cursor here.
        received_cursor: std::sync::Arc<std::sync::Mutex<Option<String>>>,
        received_search_view: std::sync::Arc<std::sync::Mutex<Option<String>>>,
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
                changes_events: vec![],
                changes_next_cursor: None,
                received_cursor: std::sync::Arc::new(std::sync::Mutex::new(None)),
                received_search_view: std::sync::Arc::new(std::sync::Mutex::new(None)),
            }
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
            self.check_fail("add_drawer")?;
            if self.add_drawer_409 {
                return Err(RemoteError::RemoteRejected {
                    remote: "mock".to_owned(),
                    status: 409,
                    body: "conflict".to_owned(),
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

        async fn kg_query(&self, _req: KgQueryRequest) -> mempalace_remote::Result<Value> {
            self.check_fail("kg_query")?;
            Ok(self.kg_query_response.clone())
        }

        async fn kg_add_fact(&self, _req: KgAddFactRequest) -> mempalace_remote::Result<Value> {
            self.check_fail("kg_add")?;
            Ok(self.kg_add_response.clone())
        }

        async fn kg_invalidate(&self, _req: KgInvalidateRequest) -> mempalace_remote::Result<Value> {
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

        async fn changes(
            &self,
            query: ChangesQuery,
        ) -> mempalace_remote::Result<ChangesResponse> {
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
    }

    impl MockRemote {
        fn check_fail(&self, endpoint: &str) -> mempalace_remote::Result<()> {
            if self.fail_on.as_deref() == Some(endpoint) {
                Err(RemoteError::Unreachable {
                    remote: "mock".to_owned(),
                    message: "mock failure".to_owned(),
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

    fn make_router(
        remotes: BTreeMap<String, Arc<dyn RemoteApi>>,
    ) -> FederationRouter {
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

        assert!(result.is_none(), "Both route should not produce remote result from add_drawer_remote");
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
        let status = router
            .add_drawer_replicate("w", "r", content, "file.txt", "agent", &route, 0.9)
            .await;

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
        mock.add_drawer_409 = true;      // but the actual add_drawer gets a 409
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
                SHARED_AGENT_DIARY_WING, "r", "diary content", "diary:general", "agent",
                &route, 0.9,
            )
            .await;

        assert_eq!(
            status,
            ReplicationStatus::Skipped,
            "diary wing should not replicate"
        );
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
                "wing_code", DIARY_ROOM, "diary content", "diary:general", "agent",
                &route, 0.9,
            )
            .await;

        assert_eq!(
            status,
            ReplicationStatus::Skipped,
            "diary room should not replicate"
        );
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
                "wing_code", "room", "diary content", "diary:standup", "agent",
                &route, 0.9,
            )
            .await;

        assert_eq!(
            status,
            ReplicationStatus::Skipped,
            "diary source_file should not replicate"
        );
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

        let local_route = ResolvedRouteRule {
            mode: RouteMode::Local,
            remote: None,
            write: WriteTarget::Local,
        };
        assert_eq!(router.resolve_write_target(&local_route), WriteTarget::Local);

        assert_eq!(router.resolve_write_target(&both_route), WriteTarget::Both);
    }

    #[tokio::test]
    async fn e2e_search_merges_and_annotates_origin() {
        let mut mock = MockRemote::default();
        mock.search_results = vec![
            json!({"wing":"w","room":"r2","similarity":0.8,"text":"remote hit"}),
        ];
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let local = vec![json!({"wing":"w","room":"r1","similarity":0.9,"text":"local hit"})];
        let result = router.search(local, "test", Some("w"), None, None, 10, &["alpha".to_owned()]).await.unwrap();

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
            .search(
                vec![],
                "test",
                None,
                None,
                Some("feature-x"),
                10,
                &["alpha".to_owned()],
            )
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
        let result = router.search(local, "test", Some("w"), None, None, 10, &["alpha".to_owned()]).await.unwrap();

        let results = result["results"].as_array().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["text"], "local only");
        let warnings = result["warnings"].as_array().unwrap();
        assert_eq!(warnings.len(), 1);
        // Use Display format (not "unreachable"); check it mentions the remote name
        assert!(warnings[0].as_str().unwrap().contains("alpha"));
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
    async fn e2e_add_drawer_local_on_local_route() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = ResolvedRouteRule {
            mode: RouteMode::Local,
            remote: None,
            write: WriteTarget::Local,
        };
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
                SHARED_AGENT_DIARY_WING, "r", "content", "file.txt", "agent",
                &route, 0.9,
            )
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "diary wing should not write remotely"
        );
    }

    #[tokio::test]
    async fn e2e_add_drawer_remote_skipped_for_diary_room() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_combined_route("alpha");
        let result = router
            .add_drawer_remote(
                "wing_code", DIARY_ROOM, "content", "file.txt", "agent",
                &route, 0.9,
            )
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "diary room should not write remotely"
        );
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
                "wing_code", "room", "content", "diary:standup", "agent",
                &route, 0.9,
            )
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "diary source_file should not write remotely"
        );
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
        router.rules.wings.insert("wing_code".to_owned(), ResolvedRouteRule {
            mode: RouteMode::Combined,
            remote: Some("alpha".to_owned()),
            write: WriteTarget::Both,
        });

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
        assert_eq!(taxonomy["room_a"], 3);  // 1 + 2
        assert_eq!(taxonomy["room_b"], 3);  // 0 + 3
        assert_eq!(taxonomy["room_c"], 4);  // 4 + 0
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
        let result = router.kg_invalidate_remote("A", "loves", "B", None, &route).await.unwrap().unwrap();
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
        let result = router
            .kg_add_remote("A", "loves", "B", None, &route)
            .await
            .unwrap();

        assert!(result.is_none(), "Both route should not produce remote result from kg_add_remote");
    }

    #[tokio::test]
    async fn e2e_kg_add_replicate_succeeds() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router
            .kg_add_replicate("A", "loves", "B", None, &route)
            .await;

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
        let status = router
            .kg_add_replicate("A", "loves", "B", None, &route)
            .await;

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
        let status = router
            .kg_add_replicate("A", "loves", "B", None, &route)
            .await;

        assert_eq!(status, ReplicationStatus::Skipped);
    }

    #[tokio::test]
    async fn e2e_kg_invalidate_remote_skips_work_for_both() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let result = router
            .kg_invalidate_remote("A", "loves", "B", None, &route)
            .await
            .unwrap();

        assert!(result.is_none(), "Both route should not produce remote result from kg_invalidate_remote");
    }

    #[tokio::test]
    async fn e2e_kg_invalidate_replicate_succeeds() {
        let mock = MockRemote::default();
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), Arc::new(mock) as Arc<dyn RemoteApi>);
        let router = make_router(remotes);

        let route = make_both_route("alpha");
        let status = router
            .kg_invalidate_replicate("A", "loves", "B", None, &route)
            .await;

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
        let status = router
            .kg_invalidate_replicate("A", "loves", "B", None, &route)
            .await;

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
        let status = router
            .kg_invalidate_replicate("A", "loves", "B", None, &route)
            .await;

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
        let router = make_router_with_rules(
            remotes, wings, RouteMode::Local, None,
        );
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
        let router = make_router_with_rules(
            remotes, wings, RouteMode::Local, None,
        );
        let (include_local, targets) =
            router.plan_search_targets(Some("wing_ext"), None);
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
        let router = make_router_with_rules(
            remotes, wings, RouteMode::Local, None,
        );
        let (include_local, targets) =
            router.plan_search_targets(Some("wing_combo"), None);
        assert!(include_local);
        assert_eq!(targets, vec!["alpha".to_owned()]);
    }

    #[test]
    fn plan_search_explicit_wing_local() {
        let mut wings = BTreeMap::new();
        wings.insert(
            "wing_local".to_owned(),
            ResolvedRouteRule {
                mode: RouteMode::Local,
                remote: None,
                write: WriteTarget::Local,
            },
        );
        let mut remotes = BTreeMap::new();
        remotes.insert("alpha".to_owned(), mock_remote_arc());
        let router = make_router_with_rules(
            remotes, wings, RouteMode::Combined, Some("alpha".to_owned()),
        );
        let (include_local, targets) =
            router.plan_search_targets(Some("wing_local"), None);
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
        let router = make_router_with_rules(
            remotes, wings, RouteMode::Local, None,
        );
        let (include_local, mut targets) =
            router.plan_search_targets(None, None);
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
        let router = make_router_with_rules(
            remotes, wings, RouteMode::Local, None,
        );
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
        let origins = vec![
            ("local".to_owned(), local),
            ("alpha".to_owned(), remote),
        ];
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
        mock.changes_events = vec![
            make_change_event("drawer_added", "2026-06-10T10:00:00Z", "entity-1"),
        ];
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
        healthy.changes_events = vec![
            make_change_event("drawer_added", "2026-06-10T11:00:00Z", "entity-ok"),
        ];

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
