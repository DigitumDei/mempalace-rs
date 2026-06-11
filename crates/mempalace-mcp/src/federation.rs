use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use mempalace_config::{
    FederationRuntimeConfig, ResolvedRouteRule, RouteMode, WriteTarget, resolve_kg_route,
    resolve_route, RouteQuery,
};
use mempalace_federation::{
    AddDrawerRequest, DrawerSearchRequest, RemoteDrawerResult,
};
use mempalace_remote::{
    RemoteApi, RemoteClient, RemoteEndpoint,
};
use serde_json::{Value, json};
use tokio::task::JoinSet;
use tracing;

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

    pub fn has_remotes(&self) -> bool {
        !self.remotes.is_empty()
    }

    /// Compute wing availability annotations for the local wing set, keyed by
    /// wing name. `local_wings` is the set of wing names present in the local
    /// palace. Returns a map of `wing_name → "local" | "remote:<name>" | "combined"`.
    pub fn wing_availability(&self, local_wings: &BTreeMap<String, usize>) -> Value {
        let mut avail = serde_json::Map::new();
        for wing_name in local_wings.keys() {
            let route = resolve_route(
                &self.rules,
                None,
                RouteQuery { wing: Some(wing_name), room: None, source_file: None },
            );
            let status = match route.mode {
                RouteMode::Local => "local",
                RouteMode::Remote => "remote",
                RouteMode::Combined => "combined",
            };
            avail.insert(wing_name.clone(), json!(status));
        }
        // Also include remote-only wings (configured but not local)
        for (name, _remote) in &self.rules.remotes {
            if !avail.contains_key(name) {
                avail.insert(name.clone(), json!("remote"));
            }
        }
        Value::Object(avail)
    }

    pub fn resolve_drawer_route(&self, wing: Option<&str>) -> ResolvedRouteRule {
        resolve_route(&self.rules, None, RouteQuery { wing, room: None, source_file: None })
    }

    pub fn resolve_kg_route(&self) -> ResolvedRouteRule {
        resolve_kg_route(&self.rules)
    }

    fn remote_for_rule(&self, rule: &ResolvedRouteRule) -> Option<&Arc<dyn RemoteApi>> {
        rule.remote.as_ref().and_then(|name| self.remotes.get(name))
    }

    // ─── Search ──────────────────────────────────────────────────────────────────

    /// Fan out search to local + remotes, merge results.
    /// Reads never hard-fail on remote outage.
    pub async fn search(
        &self,
        local_results: Vec<Value>,
        query: &str,
        wing: Option<&str>,
        room: Option<&str>,
        limit: usize,
        route: &ResolvedRouteRule,
    ) -> ToolResult<Value> {
        let (remote_results, warnings) =
            self.do_remote_search_degradable(query, wing, room, limit, route)
                .await;
        match route.mode {
            RouteMode::Local => {
                Ok(search_payload(query, wing, room, limit, local_results, &[]))
            }
            RouteMode::Remote => {
                Ok(search_payload(query, wing, room, limit, remote_results, &warnings))
            }
            RouteMode::Combined => {
                let merged = merge_search_results(local_results, remote_results, limit);
                Ok(search_payload(query, wing, room, limit, merged, &warnings))
            }
        }
    }

    async fn do_remote_search_degradable(
        &self,
        query: &str,
        wing: Option<&str>,
        room: Option<&str>,
        limit: usize,
        route: &ResolvedRouteRule,
    ) -> (Vec<Value>, Vec<String>) {
        match self.do_remote_search(query, wing, room, limit, route).await {
            Ok(results) => (results, vec![]),
            Err(e) => {
                let remote_name = route.remote.as_deref().unwrap_or("unknown");
                (vec![], vec![format!("remote `{remote_name}` unreachable: {e:?}")])
            }
        }
    }

    async fn do_remote_search(
        &self,
        query: &str,
        wing: Option<&str>,
        room: Option<&str>,
        limit: usize,
        route: &ResolvedRouteRule,
    ) -> ToolResult<Vec<Value>> {
        let Some(remote_api) = self.remote_for_rule(route) else {
            return Ok(vec![]);
        };
        let req = DrawerSearchRequest {
            query: query.to_owned(),
            wing: wing.map(|s| s.to_owned()),
            room: room.map(|s| s.to_owned()),
            limit: Some(limit),
        };
        let response = remote_api.search_drawers(req).await.map_err(|e| {
            ToolError::Internal(McpError::TimeFormat(e.to_string()))
        })?;
        let results = response
            .results
            .into_iter()
            .map(|r| drawer_result_to_value(r, route.remote.as_deref().unwrap_or("remote")))
            .collect();
        Ok(results)
    }

    // ─── Add drawer ──────────────────────────────────────────────────────────────

    /// Route an add-drawer operation. Returns `None` when the caller should
    /// execute locally; `Some(remote_result)` when routed to a remote.
    pub async fn add_drawer_remote(
        &self,
        wing: &str,
        room: &str,
        content: &str,
        source_file: &str,
        added_by: &str,
        route: &ResolvedRouteRule,
    ) -> ToolResult<Option<Value>> {
        let target_remote = match route.mode {
            RouteMode::Local => None,
            RouteMode::Remote => route.remote.as_deref(),
            RouteMode::Combined => {
                if route.write == WriteTarget::Remote {
                    route.remote.as_deref()
                } else {
                    None
                }
            }
        };
        let Some(remote_name) = target_remote else {
            return Ok(None);
        };
        let Some(api) = self.remotes.get(remote_name) else {
            return Ok(None);
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
                if resp.success {
                    Ok(Some(json!({
                        "success": true,
                        "drawer_id": resp.drawer_id,
                        "wing": resp.wing,
                        "room": resp.room,
                        "origin": remote_name,
                    })))
                } else {
                    Ok(Some(json!({
                        "success": false,
                        "reason": "duplicate",
                        "matches": [],
                        "origin": remote_name,
                    })))
                }
            }
            Err(e) => Err(ToolError::InvalidParams(format!(
                "remote `{remote_name}`: {e}"
            ))),
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
        while let Some(Ok(batch)) = set.join_next().await {
            results.extend(batch);
        }
        results
    }

    // ─── Delete drawer ───────────────────────────────────────────────────────────

    /// Try to delete a drawer from remotes in config order, after local deletion
    /// has failed. Returns the response if found on a remote.
    pub async fn delete_drawer_remote(
        &self,
        drawer_id: &str,
        route: &ResolvedRouteRule,
    ) -> ToolResult<Option<Value>> {
        let remote_names: Vec<&str> = match route.mode {
            RouteMode::Local => return Ok(None),
            RouteMode::Remote | RouteMode::Combined => {
                if let Some(name) = &route.remote {
                    vec![name.as_str()]
                } else {
                    return Ok(None);
                }
            }
        };
        for name in remote_names {
            if let Some(api) = self.remotes.get(name) {
                match api.delete_drawer(drawer_id).await {
                    Ok(()) => {
                        return Ok(Some(json!({
                            "success": true,
                            "drawer_id": drawer_id,
                            "origin": name,
                        })));
                    }
                    Err(e) if e.is_degradable() => continue,
                    Err(_) => continue,
                }
            }
        }
        Ok(None)
    }

    // ─── Taxonomy / Status ───────────────────────────────────────────────────────

    /// Fan out taxonomy, wings, and rooms queries, merging into the local payload.
    pub async fn taxonomy_merge(
        &self,
        local_taxonomy: Value,
        _route: &ResolvedRouteRule,
    ) -> ToolResult<Value> {
        let mut merged = local_taxonomy;
        for (name, api) in &self.remotes {
            match api.taxonomy().await {
                Ok(remote) => {
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
                Err(e) => {
                    tracing::warn!(
                        remote = %name,
                        %e,
                        "failed to fetch taxonomy from remote"
                    );
                }
            }
        }
        Ok(merged)
    }

    pub async fn wings_merge(
        &self,
        local_wings: Value,
        _route: &ResolvedRouteRule,
    ) -> ToolResult<Value> {
        let mut merged = local_wings;
        for (name, api) in &self.remotes {
            match api.wings().await {
                Ok(remote) => {
                    if let Some(remote_wings) = remote.get("wings") {
                        if let (Some(obj), Some(robj)) =
                            (merged.get_mut("wings").and_then(|v| v.as_object_mut()), remote_wings.as_object())
                        {
                            for (wing, count) in robj {
                                let c = count.as_u64().unwrap_or(0);
                                let entry = obj.entry(wing.clone()).or_insert(json!(0));
                                let val = entry.as_u64().unwrap_or(0);
                                *entry = json!(val + c);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(remote = %name, %e, "failed to fetch wings from remote");
                }
            }
        }
        Ok(merged)
    }

    pub async fn rooms_merge(
        &self,
        local_rooms: Value,
        wing_filter: Option<&str>,
        _route: &ResolvedRouteRule,
    ) -> ToolResult<Value> {
        let mut merged = local_rooms;
        for (name, api) in &self.remotes {
            match api.rooms(wing_filter).await {
                Ok(remote) => {
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
                Err(e) => {
                    tracing::warn!(remote = %name, %e, "failed to fetch rooms from remote");
                }
            }
        }
        Ok(merged)
    }

    pub async fn status_merge(
        &self,
        mut local_status: Value,
        _route: &ResolvedRouteRule,
    ) -> ToolResult<Value> {
        let mut federation_info = vec![];
        for (name, api) in &self.remotes {
            let remote_url = self
                .rules
                .remotes
                .get(name)
                .map(|r| r.url.as_str())
                .unwrap_or("");
            let mut entry = json!({
                "name": name,
                "url": remote_url,
                "reachable": false,
                "federation_api_version": null,
            });
            match api.info().await {
                Ok(info) => {
                    entry["reachable"] = json!(true);
                    entry["federation_api_version"] = json!(info.federation_api_version);
                }
                Err(_) => {}
            }
            federation_info.push(entry);
        }
        if let Some(obj) = local_status.as_object_mut() {
            obj.insert("federation".to_owned(), json!({ "remotes": federation_info }));
        }
        Ok(local_status)
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
            Err(_) => return Ok(local_payload),
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
        let target_remote = match route.mode {
            RouteMode::Local => None,
            RouteMode::Remote => route.remote.as_deref(),
            RouteMode::Combined => {
                if route.write == WriteTarget::Remote {
                    route.remote.as_deref()
                } else {
                    None
                }
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
            Ok(resp) => Ok(Some(resp)),
            Err(e) => Err(ToolError::InvalidParams(format!(
                "remote `{remote_name}`: {e}"
            ))),
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
        let target_remote = match route.mode {
            RouteMode::Local => None,
            RouteMode::Remote => route.remote.as_deref(),
            RouteMode::Combined => {
                if route.write == WriteTarget::Remote {
                    route.remote.as_deref()
                } else {
                    None
                }
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
            Ok(resp) => Ok(Some(resp)),
            Err(e) => Err(ToolError::InvalidParams(format!(
                "remote `{remote_name}`: {e}"
            ))),
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
            Err(_) => return Ok(local_payload),
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
            Err(_) => return Ok(local_payload),
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

/// Merge two KG stats payloads: sum numeric fields, union relationship types.
fn merge_kg_stats(local: Value, remote: Value) -> Value {
    let mut merged = local.clone();
    if let Some(obj) = merged.as_object_mut() {
        for key in &["entities", "triples", "current_facts", "expired_facts"] {
            if let (Some(local_val), Some(remote_val)) =
                (obj.get(*key), remote.get(*key))
            {
                let sum = local_val.as_u64().unwrap_or(0) + remote_val.as_u64().unwrap_or(0);
                obj[*key] = json!(sum);
            }
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
        "similarity": result.score,
        "text": result.content,
        "source_file": result.source_file,
        "origin": origin,
    });
    if let Some(c) = &result.content_hash {
        v["content_hash"] = json!(c);
    }
    v
}

fn search_payload(
    query: &str,
    wing: Option<&str>,
    room: Option<&str>,
    limit: usize,
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
        "limit": limit,
    });
    if !warnings.is_empty() {
        payload["warnings"] = json!(warnings);
    }
    payload
}

/// Merge local and remote search results by rank interleave (round-robin across
/// origins in rank order). Raw scores are never compared across embedding
/// profiles. Deduplication prefers local on content-hash match; falls back to
/// text-content matching when hashes are absent.
fn merge_search_results(
    local: Vec<Value>,
    remote: Vec<Value>,
    limit: usize,
) -> Vec<Value> {
    if local.is_empty() {
        return remote.into_iter().take(limit).collect();
    }
    if remote.is_empty() {
        return local.into_iter().take(limit).collect();
    }

    let mut merged: Vec<Value> = Vec::with_capacity(limit);
    let mut seen_hashes = std::collections::HashSet::new();
    let mut seen_texts = std::collections::HashSet::new();

    let max_rank = local.len().max(remote.len());
    for rank in 0..max_rank {
        if merged.len() >= limit {
            break;
        }
        // Interleave: local at this rank first
        if rank < local.len() {
            let item = &local[rank];
            if !is_duplicate_search_item(item, &mut seen_hashes, &mut seen_texts) {
                merged.push(item.clone());
            }
        }
        if merged.len() >= limit {
            break;
        }
        // Then remote at the same rank
        if rank < remote.len() {
            let item = &remote[rank];
            if !is_duplicate_search_item(item, &mut seen_hashes, &mut seen_texts) {
                merged.push(item.clone());
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
    // Prefer content_hash for dedup, fall back to text content.
    if let Some(hash) = item["content_hash"].as_str() {
        if !hash.is_empty() {
            return !seen_hashes.insert(hash.to_owned());
        }
    }
    if let Some(text) = item["text"].as_str() {
        return !seen_texts.insert(text.to_owned());
    }
    false
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
        let merged = merge_search_results(local, remote, 10);
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
        let merged = merge_search_results(local, remote, 10);
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
        let merged = merge_search_results(local, remote, 2);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_truncates_longer_list_to_limit() {
        let local = vec![
            json!({"wing":"w","room":"r1","similarity":0.9,"text":"a"}),
            json!({"wing":"w","room":"r2","similarity":0.8,"text":"b"}),
            json!({"wing":"w","room":"r3","similarity":0.7,"text":"c"}),
        ];
        let merged = merge_search_results(local, vec![], 2);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_empty_remote_returns_local() {
        let local =
            vec![json!({"wing":"w","room":"r1","similarity":0.9,"text":"only local"})];
        let merged = merge_search_results(local.clone(), vec![], 5);
        assert_eq!(merged, local);
    }

    #[test]
    fn merge_empty_local_returns_remote_truncated() {
        let remote = vec![
            json!({"wing":"w","room":"r1","similarity":0.9,"text":"a"}),
            json!({"wing":"w","room":"r2","similarity":0.8,"text":"b"}),
            json!({"wing":"w","room":"r3","similarity":0.7,"text":"c"}),
        ];
        let merged = merge_search_results(vec![], remote, 2);
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
        let merged = merge_search_results(local, remote, 10);
        // Local preferred on hash collision — remote skipped.
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["wing"], "w");
        assert_eq!(merged[0]["room"], "r1");
    }

    #[test]
    fn merge_dedupes_on_text_fallback() {
        let local = vec![json!({"wing":"w","room":"r1","text":"content"})];
        let remote = vec![json!({"wing":"w","room":"r2","text":"content"})];
        let merged = merge_search_results(local, remote, 10);
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
        let merged = merge_search_results(local, remote, 10);
        // L0, R0(skipped), L1, R1
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0]["text"], "alpha");
        assert_eq!(merged[1]["text"], "beta");
        assert_eq!(merged[2]["text"], "gamma");
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
}
