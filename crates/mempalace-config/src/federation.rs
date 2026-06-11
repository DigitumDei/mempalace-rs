//! Federation routing configuration — file-format types, resolved runtime
//! types, resolution/validation logic, and pure route-precedence functions.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use mempalace_core::{
    DIARY_ROOM, DIARY_TOPIC_PREFIX, MempalaceError, Result, SHARED_AGENT_DIARY_WING,
};
use serde::{Deserialize, Serialize};

/// Default connection timeout for remote connections in milliseconds.
pub const DEFAULT_REMOTE_TIMEOUT_MS: u64 = 5_000;

// ─── File-format (serde) types ────────────────────────────────────────────────

/// File-level representation of the optional `[federation]` config section.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FederationConfigV1 {
    /// List of named remote MemPalace servers.
    #[serde(default)]
    pub remotes: Vec<RemoteConfigV1>,
    /// Default routing mode when no per-wing or per-project rule matches.
    #[serde(default)]
    pub default_mode: Option<RouteMode>,
    /// Per-wing routing rules; key is the wing name.
    #[serde(default)]
    pub wings: BTreeMap<String, RouteRuleV1>,
    /// Routing rule for the knowledge graph.
    #[serde(default)]
    pub kg: Option<RouteRuleV1>,
}

/// File-level configuration for a single named remote server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteConfigV1 {
    /// Unique name identifying this remote.
    pub name: String,
    /// Base URL of the remote MemPalace server (must begin with `http://` or `https://`).
    pub url: String,
    /// Optional inline bearer token.
    #[serde(default)]
    pub token: Option<String>,
    /// Name of the environment variable whose value is used as the bearer token.
    #[serde(default)]
    pub token_env: Option<String>,
    /// Connection timeout in milliseconds (defaults to [`DEFAULT_REMOTE_TIMEOUT_MS`]).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// How requests for a particular resource are routed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteMode {
    /// Serve from the local palace only.
    Local,
    /// Serve from the configured remote only.
    Remote,
    /// Serve from both local and remote; [`WriteTarget`] controls where writes go.
    Combined,
}

/// Where write operations are directed in [`RouteMode::Combined`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteTarget {
    /// Writes go to the local palace.
    Local,
    /// Writes go to the remote server.
    Remote,
}

/// File-level routing rule for a wing or the knowledge graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteRuleV1 {
    /// Routing mode for this resource.
    pub mode: RouteMode,
    /// Name of the target remote; required when `mode` is [`RouteMode::Remote`] or
    /// [`RouteMode::Combined`].
    #[serde(default)]
    pub remote: Option<String>,
    /// Write target for [`RouteMode::Combined`]; ignored for other modes.
    #[serde(default)]
    pub write: Option<WriteTarget>,
}

// ─── Resolved runtime types ───────────────────────────────────────────────────

/// Resolved and validated federation configuration ready for runtime use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FederationRuntimeConfig {
    /// All configured remotes, keyed by name.
    pub remotes: BTreeMap<String, ResolvedRemote>,
    /// Default routing mode; [`RouteMode::Local`] when the section or field is absent.
    pub default_mode: RouteMode,
    /// Pre-resolved remote name inferred for `default_mode` when it is
    /// [`RouteMode::Remote`] or [`RouteMode::Combined`]; `None` when
    /// `default_mode` is [`RouteMode::Local`].
    pub default_remote: Option<String>,
    /// Per-wing routing rules.
    pub wings: BTreeMap<String, ResolvedRouteRule>,
    /// Knowledge-graph routing rule.
    pub kg: Option<ResolvedRouteRule>,
}

impl Default for FederationRuntimeConfig {
    fn default() -> Self {
        Self {
            remotes: BTreeMap::new(),
            default_mode: RouteMode::Local,
            default_remote: None,
            wings: BTreeMap::new(),
            kg: None,
        }
    }
}

/// A resolved remote connection definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRemote {
    /// Unique name of this remote.
    pub name: String,
    /// Trimmed base URL.
    pub url: String,
    /// Bearer token (resolved from `token_env` or inline `token`).
    pub token: Option<String>,
    /// Connection timeout.
    pub timeout: Duration,
}

/// A resolved routing rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRouteRule {
    /// Routing mode.
    pub mode: RouteMode,
    /// Target remote name; always `Some` when `mode` is not [`RouteMode::Local`].
    pub remote: Option<String>,
    /// Write target; only meaningful for [`RouteMode::Combined`].
    pub write: WriteTarget,
}

// ─── Project-level routing ────────────────────────────────────────────────────

/// Per-project routing override stored in `mempalace.yaml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectRoutingConfig {
    /// Routing mode for this project.
    pub mode: RouteMode,
    /// Named remote override.
    #[serde(default)]
    pub remote: Option<String>,
    /// Write target override.
    #[serde(default)]
    pub write: Option<WriteTarget>,
}

// ─── Route query ─────────────────────────────────────────────────────────────

/// Context used to resolve a routing rule for a specific request.
pub struct RouteQuery<'a> {
    /// The wing being accessed.
    pub wing: Option<&'a str>,
    /// The room being accessed.
    pub room: Option<&'a str>,
    /// The source file being accessed.
    pub source_file: Option<&'a str>,
}

// ─── Resolution / validation ──────────────────────────────────────────────────

/// Resolve and validate the raw [`FederationConfigV1`] section (or its absence)
/// into a [`FederationRuntimeConfig`] ready for runtime use.
///
/// `env_lookup` is called to read environment variables for `token_env` fields;
/// inject a closure over a `HashMap` in tests to avoid touching the real process
/// environment.
pub(crate) fn resolve_federation_config(
    section: Option<FederationConfigV1>,
    config_path: &Path,
    env_lookup: impl Fn(&str) -> Option<String>,
) -> Result<FederationRuntimeConfig> {
    let Some(section) = section else {
        return Ok(FederationRuntimeConfig::default());
    };

    // ── 1. Resolve remotes ────────────────────────────────────────────────────
    let mut remotes: BTreeMap<String, ResolvedRemote> = BTreeMap::new();
    for raw in section.remotes {
        if raw.name.is_empty() {
            return Err(MempalaceError::ConfigParse {
                path: config_path.to_path_buf(),
                message: "federation.remotes[].name must not be empty".to_owned(),
            });
        }
        if remotes.contains_key(&raw.name) {
            return Err(MempalaceError::ConfigParse {
                path: config_path.to_path_buf(),
                message: format!(
                    "federation.remotes contains duplicate name `{}`",
                    raw.name
                ),
            });
        }

        let url = raw.url.trim().to_owned();
        if url.is_empty() {
            return Err(MempalaceError::ConfigParse {
                path: config_path.to_path_buf(),
                message: format!(
                    "federation.remotes.{}.url must not be empty",
                    raw.name
                ),
            });
        }
        if !url.starts_with("http://") && !url.starts_with("https://") {
            return Err(MempalaceError::ConfigParse {
                path: config_path.to_path_buf(),
                message: format!(
                    "federation.remotes.{}.url `{url}` must start with http:// or https://",
                    raw.name
                ),
            });
        }

        let token = resolve_token(&raw.name, raw.token_env.as_deref(), raw.token, &env_lookup);
        let timeout =
            Duration::from_millis(raw.timeout_ms.unwrap_or(DEFAULT_REMOTE_TIMEOUT_MS));

        remotes.insert(
            raw.name.clone(),
            ResolvedRemote { name: raw.name, url, token, timeout },
        );
    }

    // ── 2. Resolve wing rules ─────────────────────────────────────────────────
    let mut wings: BTreeMap<String, ResolvedRouteRule> = BTreeMap::new();
    for (wing_name, rule) in &section.wings {
        if wing_name == SHARED_AGENT_DIARY_WING && rule.mode != RouteMode::Local {
            tracing::warn!(
                wing = %wing_name,
                "federation.wings.{wing_name} has non-Local mode; \
                 diary routing to this wing is always hard-overridden to Local"
            );
        }
        let resolved = resolve_rule(
            &format!("federation.wings.{wing_name}"),
            rule,
            &remotes,
            config_path,
        )?;
        wings.insert(wing_name.clone(), resolved);
    }

    // ── 3. Resolve kg rule ────────────────────────────────────────────────────
    let kg = match &section.kg {
        None => None,
        Some(rule) => Some(resolve_rule("federation.kg", rule, &remotes, config_path)?),
    };

    // ── 4. Resolve default_mode ───────────────────────────────────────────────
    let default_mode = section.default_mode.unwrap_or(RouteMode::Local);
    let default_remote = match default_mode {
        RouteMode::Local => None,
        RouteMode::Remote | RouteMode::Combined => {
            let name = infer_single_remote("federation.default_mode", &remotes, config_path)?;
            Some(name)
        }
    };

    Ok(FederationRuntimeConfig { remotes, default_mode, default_remote, wings, kg })
}

/// Resolve a single [`RouteRuleV1`] into a [`ResolvedRouteRule`], validating
/// remote references and applying defaults.
fn resolve_rule(
    field_path: &str,
    rule: &RouteRuleV1,
    remotes: &BTreeMap<String, ResolvedRemote>,
    config_path: &Path,
) -> Result<ResolvedRouteRule> {
    if rule.write.is_some() && rule.mode != RouteMode::Combined {
        tracing::warn!(
            field = %field_path,
            "`write` is set on a {field_path} rule whose mode is not `combined`; ignoring"
        );
    }

    let remote = match rule.mode {
        RouteMode::Local => None,
        RouteMode::Remote | RouteMode::Combined => {
            let name = match &rule.remote {
                Some(r) => {
                    if !remotes.contains_key(r.as_str()) {
                        return Err(MempalaceError::ConfigParse {
                            path: config_path.to_path_buf(),
                            message: format!(
                                "{field_path} references unknown remote `{r}`"
                            ),
                        });
                    }
                    r.clone()
                }
                None => infer_single_remote(field_path, remotes, config_path)?,
            };
            Some(name)
        }
    };

    let write = if rule.mode == RouteMode::Combined {
        rule.write.unwrap_or(WriteTarget::Local)
    } else {
        WriteTarget::Local
    };

    Ok(ResolvedRouteRule { mode: rule.mode, remote, write })
}

/// Infer the single configured remote when `remote` is absent in a rule.
/// Returns an error if there is not exactly one remote.
fn infer_single_remote(
    field_path: &str,
    remotes: &BTreeMap<String, ResolvedRemote>,
    config_path: &Path,
) -> Result<String> {
    match remotes.len() {
        1 => Ok(remotes.keys().next().expect("len is 1").clone()),
        0 => Err(MempalaceError::ConfigParse {
            path: config_path.to_path_buf(),
            message: format!(
                "{field_path} requires a remote but none are configured"
            ),
        }),
        _ => Err(MempalaceError::ConfigParse {
            path: config_path.to_path_buf(),
            message: format!(
                "{field_path} has no `remote` field; \
                 cannot infer when multiple remotes are configured"
            ),
        }),
    }
}

/// Resolve the bearer token for a remote: env var wins over inline token.
/// Warns and falls back if the env var is set but unset/empty in the environment.
fn resolve_token(
    remote_name: &str,
    token_env: Option<&str>,
    inline_token: Option<String>,
    env_lookup: &impl Fn(&str) -> Option<String>,
) -> Option<String> {
    if let Some(var_name) = token_env {
        match env_lookup(var_name) {
            Some(val) if !val.is_empty() => return Some(val),
            _ => {
                tracing::warn!(
                    remote = %remote_name,
                    env_var = %var_name,
                    "federation.remotes.{remote_name}.token_env is set but `{var_name}` \
                     is unset or empty; falling back to inline token"
                );
            }
        }
    }
    inline_token
}

// ─── Route resolution ─────────────────────────────────────────────────────────

/// Resolve the routing rule for a given request, applying the full precedence chain.
///
/// Precedence (first match wins):
/// 1. Diary hard-override (wing == [`SHARED_AGENT_DIARY_WING`], room == [`DIARY_ROOM`],
///    or source_file starts with [`DIARY_TOPIC_PREFIX`]) → always Local.
/// 2. Explicit `federation.wings[wing]` rule.
/// 3. `project_routing` override (validated at resolution time; falls through on error).
/// 4. `federation.default_mode`.
/// 5. Local.
pub fn resolve_route(
    federation: &FederationRuntimeConfig,
    project_routing: Option<&ProjectRoutingConfig>,
    query: RouteQuery<'_>,
) -> ResolvedRouteRule {
    let is_diary = query.wing == Some(SHARED_AGENT_DIARY_WING)
        || query.room == Some(DIARY_ROOM)
        || query.source_file.is_some_and(|sf| sf.starts_with(DIARY_TOPIC_PREFIX));

    // ── 1. Diary hard-override ────────────────────────────────────────────────
    if is_diary {
        // Check whether a lower-precedence rule would have routed non-locally
        // and warn if so.
        let lower_precedence = compute_lower_precedence_rule(federation, project_routing, &query);
        if lower_precedence.mode != RouteMode::Local {
            tracing::warn!(
                wing = ?query.wing,
                room = ?query.room,
                source_file = ?query.source_file,
                "diary-related request is hard-overridden to Local; \
                 a lower-precedence rule would have routed it to {:?} remote={:?}",
                lower_precedence.mode,
                lower_precedence.remote,
            );
        }
        return local_rule();
    }

    // ── 2. Explicit wing rule ─────────────────────────────────────────────────
    if let Some(wing) = query.wing {
        if let Some(rule) = federation.wings.get(wing) {
            return rule.clone();
        }
    }

    // ── 3. Project routing ────────────────────────────────────────────────────
    if let Some(proj) = project_routing {
        if let Some(rule) = resolve_project_rule(proj, federation) {
            return rule;
        }
        // else: warn already emitted inside resolve_project_rule; fall through
    }

    // ── 4. Default mode ───────────────────────────────────────────────────────
    rule_from_default_mode(federation)
}

/// Resolve the routing rule for the knowledge graph.
///
/// Precedence: `federation.kg` > `federation.default_mode` > Local.
pub fn resolve_kg_route(federation: &FederationRuntimeConfig) -> ResolvedRouteRule {
    if let Some(kg) = &federation.kg {
        return kg.clone();
    }
    rule_from_default_mode(federation)
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

/// Compute what rule would apply ignoring the diary hard-override (steps 2-5).
/// Used only for the warning in step 1.
fn compute_lower_precedence_rule(
    federation: &FederationRuntimeConfig,
    project_routing: Option<&ProjectRoutingConfig>,
    query: &RouteQuery<'_>,
) -> ResolvedRouteRule {
    if let Some(wing) = query.wing {
        if let Some(rule) = federation.wings.get(wing) {
            return rule.clone();
        }
    }
    if let Some(proj) = project_routing {
        if let Some(rule) = resolve_project_rule(proj, federation) {
            return rule;
        }
    }
    rule_from_default_mode(federation)
}

/// Attempt to resolve a project-level routing rule.
/// Returns `None` and emits a warning if the rule is invalid.
fn resolve_project_rule(
    proj: &ProjectRoutingConfig,
    federation: &FederationRuntimeConfig,
) -> Option<ResolvedRouteRule> {
    let remote = match proj.mode {
        RouteMode::Local => None,
        RouteMode::Remote | RouteMode::Combined => {
            match &proj.remote {
                Some(r) => {
                    if !federation.remotes.contains_key(r.as_str()) {
                        tracing::warn!(
                            remote = %r,
                            "project routing references unknown remote `{r}`; \
                             falling through to default_mode"
                        );
                        return None;
                    }
                    Some(r.clone())
                }
                None => {
                    // Infer from single configured remote
                    if federation.remotes.len() == 1 {
                        federation.remotes.keys().next().cloned()
                    } else {
                        tracing::warn!(
                            "project routing has no `remote` field and cannot infer \
                             (found {} remotes); falling through to default_mode",
                            federation.remotes.len()
                        );
                        return None;
                    }
                }
            }
        }
    };

    let write = if proj.mode == RouteMode::Combined {
        proj.write.unwrap_or(WriteTarget::Local)
    } else {
        WriteTarget::Local
    };

    Some(ResolvedRouteRule { mode: proj.mode, remote, write })
}

/// Build the rule implied by `federation.default_mode` (and the pre-resolved
/// `default_remote`), or return a Local rule if `default_mode` is Local.
fn rule_from_default_mode(federation: &FederationRuntimeConfig) -> ResolvedRouteRule {
    match federation.default_mode {
        RouteMode::Local => local_rule(),
        mode => ResolvedRouteRule {
            mode,
            remote: federation.default_remote.clone(),
            write: WriteTarget::Local,
        },
    }
}

/// Convenience constructor for a fully-local routing rule.
fn local_rule() -> ResolvedRouteRule {
    ResolvedRouteRule { mode: RouteMode::Local, remote: None, write: WriteTarget::Local }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::time::Duration;

    use super::*;

    // Env-injection helper: converts a slice of (&str, &str) into a closure.
    fn fake_env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        let map: HashMap<&str, &str> = pairs.iter().copied().collect();
        move |key: &str| map.get(key).map(|v| v.to_string())
    }

    fn no_env() -> impl Fn(&str) -> Option<String> {
        |_| None
    }

    fn config_path() -> &'static Path {
        Path::new("config.json")
    }

    // ── 1. Full config JSON parses and resolves correctly ─────────────────────
    #[test]
    fn full_config_json_parses_and_resolves() {
        let json = r#"{
            "federation": {
                "remotes": [
                    { "name": "work", "url": "https://palace.intra.example", "token_env": "MEMPALACE_WORK_TOKEN", "timeout_ms": 5000 }
                ],
                "default_mode": "local",
                "wings": {
                    "wing_teamdocs": { "mode": "remote", "remote": "work" },
                    "wing_bigrepo":  { "mode": "combined", "remote": "work", "write": "local" }
                },
                "kg": { "mode": "combined", "remote": "work", "write": "remote" }
            }
        }"#;

        let file: crate::config::ConfigFileV1 = serde_json::from_str(json).unwrap();
        let env = fake_env(&[("MEMPALACE_WORK_TOKEN", "tok-abc")]);
        let fed = resolve_federation_config(file.federation, config_path(), env).unwrap();

        // Remotes
        assert_eq!(fed.remotes.len(), 1);
        let work = fed.remotes.get("work").unwrap();
        assert_eq!(work.name, "work");
        assert_eq!(work.url, "https://palace.intra.example");
        assert_eq!(work.token.as_deref(), Some("tok-abc"));
        assert_eq!(work.timeout, Duration::from_millis(5000));

        // default_mode
        assert_eq!(fed.default_mode, RouteMode::Local);
        assert_eq!(fed.default_remote, None);

        // wings
        let teamdocs = fed.wings.get("wing_teamdocs").unwrap();
        assert_eq!(teamdocs.mode, RouteMode::Remote);
        assert_eq!(teamdocs.remote.as_deref(), Some("work"));
        assert_eq!(teamdocs.write, WriteTarget::Local);

        let bigrepo = fed.wings.get("wing_bigrepo").unwrap();
        assert_eq!(bigrepo.mode, RouteMode::Combined);
        assert_eq!(bigrepo.remote.as_deref(), Some("work"));
        assert_eq!(bigrepo.write, WriteTarget::Local);

        // kg
        let kg = fed.kg.as_ref().unwrap();
        assert_eq!(kg.mode, RouteMode::Combined);
        assert_eq!(kg.remote.as_deref(), Some("work"));
        assert_eq!(kg.write, WriteTarget::Remote);
    }

    // ── 2. Absent federation section → default ────────────────────────────────
    #[test]
    fn absent_federation_section_gives_defaults() {
        let fed = resolve_federation_config(None, config_path(), no_env()).unwrap();
        assert_eq!(fed, FederationRuntimeConfig::default());
        assert_eq!(fed.default_mode, RouteMode::Local);
        assert!(fed.remotes.is_empty());
        assert!(fed.wings.is_empty());
        assert!(fed.kg.is_none());
    }

    // ── 3. Unknown remote reference → ConfigParse error ───────────────────────
    #[test]
    fn unknown_remote_reference_is_error() {
        let section = FederationConfigV1 {
            remotes: vec![RemoteConfigV1 {
                name: "real".to_owned(),
                url: "https://example.com".to_owned(),
                token: None,
                token_env: None,
                timeout_ms: None,
            }],
            default_mode: None,
            wings: {
                let mut m = BTreeMap::new();
                m.insert(
                    "wing_teamdocs".to_owned(),
                    RouteRuleV1 { mode: RouteMode::Remote, remote: Some("foo".to_owned()), write: None },
                );
                m
            },
            kg: None,
        };
        let err = resolve_federation_config(Some(section), config_path(), no_env()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("federation.wings.wing_teamdocs"), "message: {msg}");
        assert!(msg.contains("foo"), "message: {msg}");
    }

    // ── 4. Single-remote inference ─────────────────────────────────────────────
    #[test]
    fn single_remote_inferred_when_rule_omits_remote() {
        let section = FederationConfigV1 {
            remotes: vec![RemoteConfigV1 {
                name: "solo".to_owned(),
                url: "https://solo.example".to_owned(),
                token: None,
                token_env: None,
                timeout_ms: None,
            }],
            default_mode: None,
            wings: {
                let mut m = BTreeMap::new();
                m.insert(
                    "wing_a".to_owned(),
                    RouteRuleV1 { mode: RouteMode::Remote, remote: None, write: None },
                );
                m
            },
            kg: None,
        };
        let fed = resolve_federation_config(Some(section), config_path(), no_env()).unwrap();
        let rule = fed.wings.get("wing_a").unwrap();
        assert_eq!(rule.remote.as_deref(), Some("solo"));
    }

    #[test]
    fn error_when_rule_omits_remote_with_two_remotes() {
        let section = FederationConfigV1 {
            remotes: vec![
                RemoteConfigV1 {
                    name: "a".to_owned(),
                    url: "https://a.example".to_owned(),
                    token: None,
                    token_env: None,
                    timeout_ms: None,
                },
                RemoteConfigV1 {
                    name: "b".to_owned(),
                    url: "https://b.example".to_owned(),
                    token: None,
                    token_env: None,
                    timeout_ms: None,
                },
            ],
            default_mode: None,
            wings: {
                let mut m = BTreeMap::new();
                m.insert(
                    "wing_x".to_owned(),
                    RouteRuleV1 { mode: RouteMode::Remote, remote: None, write: None },
                );
                m
            },
            kg: None,
        };
        let err = resolve_federation_config(Some(section), config_path(), no_env()).unwrap_err();
        assert!(err.to_string().contains("federation.wings.wing_x"), "{err}");
    }

    #[test]
    fn error_when_rule_omits_remote_with_zero_remotes() {
        let section = FederationConfigV1 {
            remotes: vec![],
            default_mode: None,
            wings: {
                let mut m = BTreeMap::new();
                m.insert(
                    "wing_x".to_owned(),
                    RouteRuleV1 { mode: RouteMode::Remote, remote: None, write: None },
                );
                m
            },
            kg: None,
        };
        let err = resolve_federation_config(Some(section), config_path(), no_env()).unwrap_err();
        assert!(err.to_string().contains("federation.wings.wing_x"), "{err}");
    }

    // ── 5. default_mode inference ─────────────────────────────────────────────
    #[test]
    fn default_mode_remote_with_two_remotes_is_error() {
        let section = FederationConfigV1 {
            remotes: vec![
                RemoteConfigV1 {
                    name: "a".to_owned(),
                    url: "https://a.example".to_owned(),
                    token: None,
                    token_env: None,
                    timeout_ms: None,
                },
                RemoteConfigV1 {
                    name: "b".to_owned(),
                    url: "https://b.example".to_owned(),
                    token: None,
                    token_env: None,
                    timeout_ms: None,
                },
            ],
            default_mode: Some(RouteMode::Remote),
            wings: BTreeMap::new(),
            kg: None,
        };
        let err = resolve_federation_config(Some(section), config_path(), no_env()).unwrap_err();
        assert!(err.to_string().contains("federation.default_mode"), "{err}");
    }

    #[test]
    fn default_mode_remote_with_one_remote_resolves() {
        let section = FederationConfigV1 {
            remotes: vec![RemoteConfigV1 {
                name: "solo".to_owned(),
                url: "https://solo.example".to_owned(),
                token: None,
                token_env: None,
                timeout_ms: None,
            }],
            default_mode: Some(RouteMode::Remote),
            wings: BTreeMap::new(),
            kg: None,
        };
        let fed = resolve_federation_config(Some(section), config_path(), no_env()).unwrap();
        assert_eq!(fed.default_mode, RouteMode::Remote);
        assert_eq!(fed.default_remote.as_deref(), Some("solo"));
    }

    // ── 6. Token resolution ───────────────────────────────────────────────────
    #[test]
    fn token_env_wins_over_inline_token() {
        let section = FederationConfigV1 {
            remotes: vec![RemoteConfigV1 {
                name: "r".to_owned(),
                url: "https://example.com".to_owned(),
                token: Some("inline-tok".to_owned()),
                token_env: Some("MY_TOKEN_VAR".to_owned()),
                timeout_ms: None,
            }],
            default_mode: None,
            wings: BTreeMap::new(),
            kg: None,
        };
        let env = fake_env(&[("MY_TOKEN_VAR", "env-tok")]);
        let fed = resolve_federation_config(Some(section), config_path(), env).unwrap();
        assert_eq!(fed.remotes["r"].token.as_deref(), Some("env-tok"));
    }

    #[test]
    fn token_env_missing_falls_back_to_inline() {
        let section = FederationConfigV1 {
            remotes: vec![RemoteConfigV1 {
                name: "r".to_owned(),
                url: "https://example.com".to_owned(),
                token: Some("inline-tok".to_owned()),
                token_env: Some("MISSING_VAR".to_owned()),
                timeout_ms: None,
            }],
            default_mode: None,
            wings: BTreeMap::new(),
            kg: None,
        };
        let fed = resolve_federation_config(Some(section), config_path(), no_env()).unwrap();
        assert_eq!(fed.remotes["r"].token.as_deref(), Some("inline-tok"));
    }

    #[test]
    fn no_token_gives_none() {
        let section = FederationConfigV1 {
            remotes: vec![RemoteConfigV1 {
                name: "r".to_owned(),
                url: "https://example.com".to_owned(),
                token: None,
                token_env: None,
                timeout_ms: None,
            }],
            default_mode: None,
            wings: BTreeMap::new(),
            kg: None,
        };
        let fed = resolve_federation_config(Some(section), config_path(), no_env()).unwrap();
        assert_eq!(fed.remotes["r"].token, None);
    }

    // ── 7. timeout_ms absent → 5s default ────────────────────────────────────
    #[test]
    fn timeout_ms_absent_defaults_to_5s() {
        let section = FederationConfigV1 {
            remotes: vec![RemoteConfigV1 {
                name: "r".to_owned(),
                url: "https://example.com".to_owned(),
                token: None,
                token_env: None,
                timeout_ms: None,
            }],
            default_mode: None,
            wings: BTreeMap::new(),
            kg: None,
        };
        let fed = resolve_federation_config(Some(section), config_path(), no_env()).unwrap();
        assert_eq!(fed.remotes["r"].timeout, Duration::from_millis(5_000));
    }

    // ── 8. RouteMode / WriteTarget serde round-trip ───────────────────────────
    #[test]
    fn route_mode_serde_roundtrip() {
        assert_eq!(
            serde_json::from_str::<RouteMode>("\"local\"").unwrap(),
            RouteMode::Local
        );
        assert_eq!(
            serde_json::from_str::<RouteMode>("\"remote\"").unwrap(),
            RouteMode::Remote
        );
        assert_eq!(
            serde_json::from_str::<RouteMode>("\"combined\"").unwrap(),
            RouteMode::Combined
        );
        assert_eq!(serde_json::to_string(&RouteMode::Local).unwrap(), "\"local\"");
        assert_eq!(serde_json::to_string(&RouteMode::Remote).unwrap(), "\"remote\"");
        assert_eq!(serde_json::to_string(&RouteMode::Combined).unwrap(), "\"combined\"");
    }

    #[test]
    fn write_target_serde_roundtrip() {
        assert_eq!(
            serde_json::from_str::<WriteTarget>("\"local\"").unwrap(),
            WriteTarget::Local
        );
        assert_eq!(
            serde_json::from_str::<WriteTarget>("\"remote\"").unwrap(),
            WriteTarget::Remote
        );
        assert_eq!(serde_json::to_string(&WriteTarget::Local).unwrap(), "\"local\"");
        assert_eq!(serde_json::to_string(&WriteTarget::Remote).unwrap(), "\"remote\"");
    }

    // ── 9. ProjectConfig YAML with routing ────────────────────────────────────
    #[test]
    fn project_config_with_routing_parses() {
        let yaml = "wing: project_alpha\nrouting:\n  mode: combined\n  remote: work\n";
        let config: crate::config::ProjectConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.wing, "project_alpha");
        let routing = config.routing.unwrap();
        assert_eq!(routing.mode, RouteMode::Combined);
        assert_eq!(routing.remote.as_deref(), Some("work"));
        assert_eq!(routing.write, None);
    }

    #[test]
    fn project_config_without_routing_is_back_compat() {
        let yaml = "wing: project_beta\nrooms:\n  - name: backend\n";
        let config: crate::config::ProjectConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.wing, "project_beta");
        assert!(config.routing.is_none());
    }

    // ── 10. TABLE-DRIVEN precedence test for resolve_route ───────────────────
    fn make_federation_with_remote(remote_name: &str, remote_url: &str) -> FederationRuntimeConfig {
        let mut remotes = BTreeMap::new();
        remotes.insert(
            remote_name.to_owned(),
            ResolvedRemote {
                name: remote_name.to_owned(),
                url: remote_url.to_owned(),
                token: None,
                timeout: Duration::from_secs(5),
            },
        );
        FederationRuntimeConfig {
            remotes,
            default_mode: RouteMode::Local,
            default_remote: None,
            wings: BTreeMap::new(),
            kg: None,
        }
    }

    fn work_remote_federation() -> FederationRuntimeConfig {
        make_federation_with_remote("work", "https://work.example")
    }

    #[test]
    fn resolve_route_table_driven() {
        struct Case {
            name: &'static str,
            federation: FederationRuntimeConfig,
            project_routing: Option<ProjectRoutingConfig>,
            query_wing: Option<&'static str>,
            query_room: Option<&'static str>,
            query_source_file: Option<&'static str>,
            expected: ResolvedRouteRule,
        }

        // ── Build federation fixtures ─────────────────────────────────────────
        let empty_fed = FederationRuntimeConfig::default();

        let fed_with_wing_rule = {
            let base = work_remote_federation();
            let mut wings = BTreeMap::new();
            wings.insert(
                "wing_teamdocs".to_owned(),
                ResolvedRouteRule {
                    mode: RouteMode::Remote,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Local,
                },
            );
            FederationRuntimeConfig { wings, ..base }
        };

        let fed_default_remote = {
            let base = work_remote_federation();
            FederationRuntimeConfig {
                default_mode: RouteMode::Remote,
                default_remote: Some("work".to_owned()),
                ..base
            }
        };

        let fed_with_diary_wing_remote_rule = {
            let base = work_remote_federation();
            let mut wings = BTreeMap::new();
            wings.insert(
                SHARED_AGENT_DIARY_WING.to_owned(),
                ResolvedRouteRule {
                    mode: RouteMode::Remote,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Local,
                },
            );
            FederationRuntimeConfig { wings, ..base }
        };

        let fed_with_wing_write_remote = {
            let base = work_remote_federation();
            let mut wings = BTreeMap::new();
            wings.insert(
                "wing_combo".to_owned(),
                ResolvedRouteRule {
                    mode: RouteMode::Combined,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Remote,
                },
            );
            FederationRuntimeConfig { wings, ..base }
        };

        let fed_with_wing_no_write = {
            let base = work_remote_federation();
            let mut wings = BTreeMap::new();
            wings.insert(
                "wing_combo2".to_owned(),
                ResolvedRouteRule {
                    mode: RouteMode::Combined,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Local,
                },
            );
            FederationRuntimeConfig { wings, ..base }
        };

        let cases: Vec<Case> = vec![
            // 1. Empty federation, no project → Local
            Case {
                name: "empty federation, no project → Local",
                federation: empty_fed.clone(),
                project_routing: None,
                query_wing: Some("wing_foo"),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Local,
                    remote: None,
                    write: WriteTarget::Local,
                },
            },
            // 2. Explicit wing rule remote:work → Remote(work)
            Case {
                name: "explicit wing rule remote:work → Remote(work)",
                federation: fed_with_wing_rule.clone(),
                project_routing: None,
                query_wing: Some("wing_teamdocs"),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Remote,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Local,
                },
            },
            // 3. Wing rule AND project routing → wing rule wins
            Case {
                name: "wing rule AND project routing → wing rule wins",
                federation: fed_with_wing_rule.clone(),
                project_routing: Some(ProjectRoutingConfig {
                    mode: RouteMode::Combined,
                    remote: Some("work".to_owned()),
                    write: None,
                }),
                query_wing: Some("wing_teamdocs"),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Remote,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Local,
                },
            },
            // 4. No wing rule, project combined/work/write:local → Combined(work, write Local)
            Case {
                name: "no wing rule, project combined/work/write:local → Combined(work, Local)",
                federation: work_remote_federation(),
                project_routing: Some(ProjectRoutingConfig {
                    mode: RouteMode::Combined,
                    remote: Some("work".to_owned()),
                    write: Some(WriteTarget::Local),
                }),
                query_wing: Some("wing_other"),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Combined,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Local,
                },
            },
            // 5. No wing/project, default_mode remote w/ single remote → Remote(inferred)
            Case {
                name: "default_mode remote single remote → Remote(work)",
                federation: fed_default_remote.clone(),
                project_routing: None,
                query_wing: Some("wing_other"),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Remote,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Local,
                },
            },
            // 6. wing == "wing_agents" with explicit remote rule → Local (hard override)
            Case {
                name: "wing_agents with remote rule → Local hard override",
                federation: fed_with_diary_wing_remote_rule.clone(),
                project_routing: None,
                query_wing: Some(SHARED_AGENT_DIARY_WING),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Local,
                    remote: None,
                    write: WriteTarget::Local,
                },
            },
            // 7. room == "diary" under a remoted wing → Local (hard override)
            Case {
                name: "room == diary → Local hard override",
                federation: fed_default_remote.clone(),
                project_routing: None,
                query_wing: Some("wing_foo"),
                query_room: Some(DIARY_ROOM),
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Local,
                    remote: None,
                    write: WriteTarget::Local,
                },
            },
            // 8. source_file starts with diary: → Local (hard override)
            Case {
                name: "source_file = diary:standup → Local hard override",
                federation: fed_default_remote.clone(),
                project_routing: None,
                query_wing: None,
                query_room: None,
                query_source_file: Some("diary:standup"),
                expected: ResolvedRouteRule {
                    mode: RouteMode::Local,
                    remote: None,
                    write: WriteTarget::Local,
                },
            },
            // 9. Combined rule write:remote → write Remote
            Case {
                name: "combined rule write:remote → write Remote",
                federation: fed_with_wing_write_remote.clone(),
                project_routing: None,
                query_wing: Some("wing_combo"),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Combined,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Remote,
                },
            },
            // 10. Combined rule write omitted → write Local
            Case {
                name: "combined rule write omitted → write Local",
                federation: fed_with_wing_no_write.clone(),
                project_routing: None,
                query_wing: Some("wing_combo2"),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Combined,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Local,
                },
            },
            // 11. Project routing names unknown remote → falls through to default_mode local → Local
            Case {
                name: "project routing unknown remote → falls through → Local",
                federation: work_remote_federation(),
                project_routing: Some(ProjectRoutingConfig {
                    mode: RouteMode::Remote,
                    remote: Some("nonexistent".to_owned()),
                    write: None,
                }),
                query_wing: Some("wing_other"),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Local,
                    remote: None,
                    write: WriteTarget::Local,
                },
            },
            // 12. Wing not in rules, no project, no default_mode → Local
            Case {
                name: "wing not in rules, no project, no default_mode → Local",
                federation: work_remote_federation(),
                project_routing: None,
                query_wing: Some("wing_unknown"),
                query_room: None,
                query_source_file: None,
                expected: ResolvedRouteRule {
                    mode: RouteMode::Local,
                    remote: None,
                    write: WriteTarget::Local,
                },
            },
        ];

        for case in &cases {
            let result = resolve_route(
                &case.federation,
                case.project_routing.as_ref(),
                RouteQuery {
                    wing: case.query_wing,
                    room: case.query_room,
                    source_file: case.query_source_file,
                },
            );
            assert_eq!(result, case.expected, "CASE: {}", case.name);
        }
    }

    // ── 11. resolve_kg_route ──────────────────────────────────────────────────
    #[test]
    fn resolve_kg_route_cases() {
        // kg rule present → use it
        let fed_with_kg = {
            let base = work_remote_federation();
            FederationRuntimeConfig {
                kg: Some(ResolvedRouteRule {
                    mode: RouteMode::Remote,
                    remote: Some("work".to_owned()),
                    write: WriteTarget::Local,
                }),
                ..base
            }
        };
        let result = resolve_kg_route(&fed_with_kg);
        assert_eq!(result.mode, RouteMode::Remote);
        assert_eq!(result.remote.as_deref(), Some("work"));

        // kg absent + default_mode remote(single remote) → that
        let fed_default_remote = FederationRuntimeConfig {
            default_mode: RouteMode::Remote,
            default_remote: Some("work".to_owned()),
            ..work_remote_federation()
        };
        let result = resolve_kg_route(&fed_default_remote);
        assert_eq!(result.mode, RouteMode::Remote);
        assert_eq!(result.remote.as_deref(), Some("work"));

        // both absent → Local
        let fed_empty = FederationRuntimeConfig::default();
        let result = resolve_kg_route(&fed_empty);
        assert_eq!(result.mode, RouteMode::Local);
        assert_eq!(result.remote, None);
    }
}
