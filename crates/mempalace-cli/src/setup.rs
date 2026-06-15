//! `mempalace setup` — detect installed AI coding tools and register the
//! mempalace MCP server with each, idempotently.
//!
//! The MCP server is the local `mempalace-mcp` binary (by default
//! `~/.mempalace/bin/mempalace-mcp[.exe]`), launched over stdio. For each
//! supported tool we either run its own `mcp add` CLI (Claude Code, Codex,
//! Gemini CLI) or merge an entry into its JSON config file (opencode, GitHub
//! Copilot CLI, Antigravity). Cloud-only agents that cannot run a local stdio
//! server (Jules) are reported as unsupported and skipped.
//!
//! The per-tool config mechanisms (paths, schemas, CLI commands) were verified
//! against each tool's official documentation; see the PR for sources.

use std::ffi::OsStr;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};

/// All tool keys handled by `setup`, in display order.
const ALL_TOOLS: &[&str] = &[
    "claude",
    "codex",
    "gemini",
    "opencode",
    "copilot",
    "antigravity",
    "jules",
];

/// Options parsed from the `setup` subcommand.
pub struct SetupOptions {
    /// Override for the mempalace-mcp binary path. When `None`, defaults to
    /// `~/.mempalace/bin/mempalace-mcp[.exe]`.
    pub mcp_path: Option<PathBuf>,
    /// Preview changes without running commands or writing files.
    pub dry_run: bool,
    /// Restrict to these tool keys (case-insensitive). `None` = all.
    pub only: Option<Vec<String>>,
}

/// Outcome of attempting to configure a single tool.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Status {
    /// Newly registered / config written.
    Configured,
    /// Already registered with the same entry — no change.
    AlreadyConfigured,
    /// Dry-run: what would have happened.
    WouldConfigure,
    /// Tool is not installed on this machine.
    NotInstalled,
    /// Tool exists but cannot host a local stdio MCP server.
    Unsupported,
    /// Detected and attempted, but the attempt failed.
    Failed,
}

/// Per-tool result.
#[derive(Debug, Clone)]
pub struct ToolOutcome {
    pub key: &'static str,
    pub status: Status,
    pub detail: String,
}

/// Aggregate result of a `setup` run.
pub struct SetupReport {
    pub mcp_path: PathBuf,
    pub mcp_binary_present: bool,
    pub dry_run: bool,
    pub outcomes: Vec<ToolOutcome>,
}

/// Run setup across the selected tools and return a structured report.
pub fn run_setup(opts: &SetupOptions) -> SetupReport {
    let mcp_path = resolve_mcp_binary(opts.mcp_path.as_deref());
    let mcp_present = mcp_path.as_ref().map(|p| p.is_file()).unwrap_or(false);
    let mcp_str = mcp_path
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();

    let selected: Vec<&'static str> = match &opts.only {
        Some(list) => ALL_TOOLS
            .iter()
            .copied()
            .filter(|k| list.iter().any(|s| s.trim().eq_ignore_ascii_case(k)))
            .collect(),
        None => ALL_TOOLS.to_vec(),
    };

    let outcomes = selected
        .into_iter()
        .map(|key| handle_tool(key, &mcp_str, opts.dry_run))
        .collect();

    SetupReport {
        mcp_path: mcp_path.unwrap_or_default(),
        mcp_binary_present: mcp_present,
        dry_run: opts.dry_run,
        outcomes,
    }
}

impl SetupReport {
    /// Render a human-readable summary for the CLI.
    pub fn render(&self) -> String {
        let width = 55;
        let mut lines = vec![
            format!("\n{}", "=".repeat(width)),
            "  MemPalace setup".to_owned(),
            "=".repeat(width),
            format!("  MCP server: {}", self.mcp_path.display()),
        ];
        if !self.mcp_binary_present {
            lines.push(
                "  ! binary not found at that path yet — tools are pointed here;".to_owned(),
            );
            lines.push("    install mempalace-mcp there for them to launch it.".to_owned());
        }
        lines.push(String::new());

        for o in &self.outcomes {
            let glyph = match o.status {
                Status::Configured => "+",
                Status::AlreadyConfigured => "=",
                Status::WouldConfigure => "~",
                Status::NotInstalled => "-",
                Status::Unsupported => "x",
                Status::Failed => "!",
            };
            lines.push(format!("  {glyph} {:<12} {}", o.key, o.detail));
        }

        lines.push(format!("\n{}", "=".repeat(width)));
        if self.dry_run {
            lines.push("  (dry run — nothing was written)".to_owned());
        }
        lines.push(String::new());
        lines.join("\n")
    }
}

fn handle_tool(key: &'static str, mcp: &str, dry_run: bool) -> ToolOutcome {
    if mcp.is_empty() {
        return outcome(key, Status::Failed, "could not resolve a mempalace-mcp path (no home directory; pass --mcp-path)");
    }
    match key {
        "jules" => {
            // Jules is a cloud agent; it only supports a curated set of remote
            // SaaS MCP integrations configured in its web UI. No local stdio.
            if find_on_path("jules").is_some() {
                outcome(
                    key,
                    Status::Unsupported,
                    "cloud-only: configure MCP in the Jules web UI; it can't run a local server",
                )
            } else {
                not_installed(key)
            }
        }
        // CLI-driven tools need the binary on PATH (we run its `mcp add`); the
        // resolved path is invoked directly so npm `.cmd` shims work without a
        // `cmd /C` layer (which would mangle `^`/`%` in the MCP path on Windows).
        "claude" => match find_on_path("claude") {
            Some(bin) => configure_claude(&bin, mcp, dry_run),
            None => not_installed(key),
        },
        "codex" => match find_on_path("codex") {
            Some(bin) => configure_codex(&bin, mcp, dry_run),
            None => not_installed(key),
        },
        "gemini" => match find_on_path("gemini") {
            Some(bin) => configure_gemini(&bin, mcp, dry_run),
            None => not_installed(key),
        },
        "opencode" => {
            if find_on_path("opencode").is_none() {
                return not_installed(key);
            }
            configure_opencode(mcp, dry_run)
        }
        "copilot" => {
            if find_on_path("copilot").is_none() && !home_subdir_exists(".copilot") {
                return not_installed(key);
            }
            configure_copilot(mcp, dry_run)
        }
        "antigravity" => {
            if !detect_antigravity() {
                return not_installed(key);
            }
            configure_antigravity(mcp, dry_run)
        }
        other => outcome(other, Status::Failed, "unknown tool"),
    }
}

// ─── CLI-based tools ─────────────────────────────────────────────────────────

fn configure_claude(bin: &Path, mcp: &str, dry_run: bool) -> ToolOutcome {
    if dry_run {
        return outcome(
            "claude",
            Status::WouldConfigure,
            format!("would run: claude mcp add --scope user mempalace -- {mcp}"),
        );
    }
    // `claude mcp get` exits non-zero when the server is absent.
    if let Ok(out) = run_tool(bin, &["mcp", "get", "mempalace"]) {
        if out.status.success() {
            return outcome("claude", Status::AlreadyConfigured, "already registered (user scope)");
        }
    }
    match run_tool(bin, &["mcp", "add", "--scope", "user", "mempalace", "--", mcp]) {
        Ok(out) if out.status.success() => {
            outcome("claude", Status::Configured, "registered via `claude mcp add --scope user`")
        }
        Ok(out) => outcome("claude", Status::Failed, format!("`claude mcp add` failed: {}", stderr_snippet(&out))),
        Err(e) => outcome("claude", Status::Failed, format!("could not run claude: {e}")),
    }
}

fn configure_codex(bin: &Path, mcp: &str, dry_run: bool) -> ToolOutcome {
    if dry_run {
        return outcome(
            "codex",
            Status::WouldConfigure,
            format!("would run: codex mcp add mempalace -- {mcp}"),
        );
    }
    if let Ok(out) = run_tool(bin, &["mcp", "get", "mempalace"]) {
        if out.status.success() {
            return outcome("codex", Status::AlreadyConfigured, "already registered (~/.codex/config.toml)");
        }
    }
    match run_tool(bin, &["mcp", "add", "mempalace", "--", mcp]) {
        Ok(out) if out.status.success() => {
            outcome("codex", Status::Configured, "registered via `codex mcp add`")
        }
        Ok(out) => outcome("codex", Status::Failed, format!("`codex mcp add` failed: {}", stderr_snippet(&out))),
        Err(e) => outcome("codex", Status::Failed, format!("could not run codex: {e}")),
    }
}

fn configure_gemini(bin: &Path, mcp: &str, dry_run: bool) -> ToolOutcome {
    if dry_run {
        return outcome(
            "gemini",
            Status::WouldConfigure,
            format!("would run: gemini mcp add -s user mempalace \"{mcp}\""),
        );
    }
    // `gemini mcp add` errors on a duplicate name, so check the list first.
    // Match the name as a token on its own line — a bare substring would
    // false-positive on `mempalace-old` or on the `~/.mempalace/...` binary path.
    if let Ok(out) = run_tool(bin, &["mcp", "list"]) {
        if out.status.success()
            && list_contains_server(&String::from_utf8_lossy(&out.stdout), "mempalace")
        {
            return outcome("gemini", Status::AlreadyConfigured, "already registered (user scope)");
        }
    }
    match run_tool(bin, &["mcp", "add", "-s", "user", "mempalace", mcp]) {
        Ok(out) if out.status.success() => {
            outcome("gemini", Status::Configured, "registered via `gemini mcp add -s user`")
        }
        Ok(out) => outcome("gemini", Status::Failed, format!("`gemini mcp add` failed: {}", stderr_snippet(&out))),
        Err(e) => outcome("gemini", Status::Failed, format!("could not run gemini: {e}")),
    }
}

/// Does `gemini mcp list` output list a server named exactly `name`? Matches the
/// name as a line-leading token (after stripping status glyphs/indentation), so
/// it won't be fooled by `mempalace-old`, `my-mempalace`, or a command path that
/// merely contains the string.
fn list_contains_server(stdout: &str, name: &str) -> bool {
    stdout.lines().any(|line| {
        let token = line.trim_start_matches(|c: char| !c.is_alphanumeric());
        if token == name {
            return true;
        }
        match token.strip_prefix(name) {
            // Followed by a name-continuation char => a different, longer name.
            Some(rest) => !rest.starts_with(|c: char| c.is_alphanumeric() || c == '-' || c == '_'),
            None => false,
        }
    })
}

// ─── Config-file tools ───────────────────────────────────────────────────────

fn configure_opencode(mcp: &str, dry_run: bool) -> ToolOutcome {
    let Some(path) = opencode_config_path() else {
        return outcome("opencode", Status::Failed, "could not resolve the opencode config directory");
    };
    // opencode: top-level "mcp", stdio server is type "local" with command as
    // a single [exe, ...args] array.
    let entry = json!({ "type": "local", "command": [mcp], "enabled": true });
    apply_json("opencode", &path, "mcp", entry, dry_run)
}

fn configure_copilot(mcp: &str, dry_run: bool) -> ToolOutcome {
    let Some(path) = home_dir().map(|h| h.join(".copilot").join("mcp-config.json")) else {
        return outcome("copilot", Status::Failed, "could not resolve the home directory");
    };
    let entry = json!({
        "type": "local",
        "command": mcp,
        "args": [],
        "env": {},
        "tools": ["*"],
    });
    apply_json("copilot", &path, "mcpServers", entry, dry_run)
}

fn configure_antigravity(mcp: &str, dry_run: bool) -> ToolOutcome {
    let Some(gemini) = home_dir().map(|h| h.join(".gemini")) else {
        return outcome("antigravity", Status::Failed, "could not resolve the home directory");
    };
    let entry = json!({ "command": mcp });
    // Antigravity's MCP config location has shifted across versions: the shared
    // "central" config at ~/.gemini/config/mcp_config.json and the CLI's own
    // ~/.gemini/antigravity-cli/mcp_config.json. Write both — an extra inert
    // file is harmless, and this works whichever the installed build reads.
    let targets = [
        gemini.join("config").join("mcp_config.json"),
        gemini.join("antigravity-cli").join("mcp_config.json"),
    ];
    combine_outcomes(
        "antigravity",
        targets
            .iter()
            .map(|path| apply_json("antigravity", path, "mcpServers", entry.clone(), dry_run)),
    )
}

/// Fold several per-file outcomes (one tool writing multiple config files) into
/// one. Prefer the most "positive" status so a success isn't hidden by a
/// best-effort secondary write; concatenate the details.
fn combine_outcomes(
    key: &'static str,
    outcomes: impl Iterator<Item = ToolOutcome>,
) -> ToolOutcome {
    let list: Vec<ToolOutcome> = outcomes.collect();
    let has = |s: Status| list.iter().any(|o| o.status == s);
    let status = if has(Status::Configured) {
        Status::Configured
    } else if has(Status::WouldConfigure) {
        Status::WouldConfigure
    } else if has(Status::AlreadyConfigured) {
        Status::AlreadyConfigured
    } else {
        Status::Failed
    };
    let detail = list.iter().map(|o| o.detail.as_str()).collect::<Vec<_>>().join("; ");
    outcome(key, status, detail)
}

/// Read-modify-write a JSON config file, upserting one MCP server entry.
fn apply_json(
    key: &'static str,
    path: &Path,
    top_key: &str,
    entry: Value,
    dry_run: bool,
) -> ToolOutcome {
    let existing = match read_json(path) {
        Ok(v) => v,
        Err(ReadError::Io(e)) => {
            return outcome(key, Status::Failed, format!("could not read {}: {e}", path.display()));
        }
        Err(ReadError::Parse(e)) => {
            return outcome(
                key,
                Status::Failed,
                format!("{} is not valid JSON ({e}); add mempalace manually to avoid clobbering it", path.display()),
            );
        }
    };

    let (merged, changed) = upsert_mcp_entry(existing, top_key, "mempalace", &entry);
    if !changed {
        return outcome(key, Status::AlreadyConfigured, format!("already present in {}", path.display()));
    }
    if dry_run {
        return outcome(key, Status::WouldConfigure, format!("would write {}", path.display()));
    }
    match write_json(path, &merged) {
        Ok(()) => outcome(key, Status::Configured, format!("wrote {}", path.display())),
        Err(e) => outcome(key, Status::Failed, format!("could not write {}: {e}", path.display())),
    }
}

enum ReadError {
    Io(io::Error),
    Parse(serde_json::Error),
}

/// Read a JSON file into a `Value`, or `None` when the file does not exist.
/// Attempts the read directly and treats `NotFound` as absent, rather than a
/// separate `exists()` check (which would be a TOCTOU race).
fn read_json(path: &Path) -> Result<Option<Value>, ReadError> {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(ReadError::Io(e)),
    };
    if text.trim().is_empty() {
        return Ok(None);
    }
    serde_json::from_str(&text).map(Some).map_err(ReadError::Parse)
}

fn write_json(path: &Path, value: &Value) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut text = serde_json::to_string_pretty(value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    text.push('\n');
    // Write to a sibling temp file then rename into place, so an interrupted
    // write (crash, power loss, disk full) can't leave a tool's config
    // truncated/corrupt. rename is atomic within the same directory/filesystem.
    let tmp = path.with_extension("mempalace-tmp");
    fs::write(&tmp, text)?;
    fs::rename(&tmp, path)
}

/// Upsert `top_key -> name -> entry` into a JSON object, preserving every other
/// key. Returns the merged value and whether it changed (idempotency signal).
fn upsert_mcp_entry(existing: Option<Value>, top_key: &str, name: &str, entry: &Value) -> (Value, bool) {
    let mut root = match existing {
        Some(Value::Object(map)) => map,
        _ => serde_json::Map::new(),
    };

    // Ensure top_key holds an object.
    let had_object = root.get(top_key).map(Value::is_object).unwrap_or(false);
    if !had_object {
        root.insert(top_key.to_owned(), Value::Object(serde_json::Map::new()));
    }

    let mut changed = !had_object;
    if let Some(Value::Object(servers)) = root.get_mut(top_key) {
        if servers.get(name) != Some(entry) {
            servers.insert(name.to_owned(), entry.clone());
            changed = true;
        }
    }
    (Value::Object(root), changed)
}

// ─── Detection + path helpers ────────────────────────────────────────────────

fn detect_antigravity() -> bool {
    if find_on_path("antigravity").is_some() {
        return true;
    }
    if cfg!(target_os = "macos") && Path::new("/Applications/Antigravity.app").exists() {
        return true;
    }
    if cfg!(windows) {
        if let Some(local) = std::env::var_os("LOCALAPPDATA") {
            if Path::new(&local).join("Programs").join("Antigravity").exists() {
                return true;
            }
        }
    }
    home_dir().map(|h| antigravity_home_marker(&h)).unwrap_or(false)
}

/// Antigravity-owned signal under `~/.gemini`. We key on `antigravity-cli/`,
/// NOT the bare `config/` dir — the latter is shared with (and created by) the
/// Gemini CLI's migration store, so it would false-positive for Gemini-only users.
fn antigravity_home_marker(home: &Path) -> bool {
    home.join(".gemini").join("antigravity-cli").exists()
}

fn opencode_config_path() -> Option<PathBuf> {
    // opencode uses XDG even on Windows (home/.config/opencode). opencode loads
    // both opencode.json and opencode.jsonc; we only ever touch the strict-JSON
    // opencode.json so our serde_json parser and the file format always agree —
    // a hand-authored .jsonc with comments could not round-trip through it.
    Some(config_home()?.join("opencode").join("opencode.json"))
}

fn config_home() -> Option<PathBuf> {
    if let Some(xdg) = std::env::var_os("XDG_CONFIG_HOME") {
        if !xdg.is_empty() {
            return Some(PathBuf::from(xdg));
        }
    }
    home_dir().map(|h| h.join(".config"))
}

fn home_dir() -> Option<PathBuf> {
    dirs::home_dir()
}

fn home_subdir_exists(name: &str) -> bool {
    home_dir().map(|h| h.join(name).exists()).unwrap_or(false)
}

fn resolve_mcp_binary(override_path: Option<&Path>) -> Option<PathBuf> {
    if let Some(p) = override_path {
        return Some(p.to_path_buf());
    }
    let bin = if cfg!(windows) { "mempalace-mcp.exe" } else { "mempalace-mcp" };
    home_dir().map(|h| h.join(".mempalace").join("bin").join(bin))
}

/// Locate an executable named `name` on `PATH`.
fn find_on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    lookup_in_paths(name, &path, cfg!(windows))
}

/// PATH lookup split out for testing (no env access). On Windows, also tries the
/// common executable extensions so npm shims (`claude.cmd`, `gemini.cmd`) resolve.
fn lookup_in_paths(name: &str, path_var: &OsStr, windows: bool) -> Option<PathBuf> {
    let exts: &[&str] = if windows {
        &["", ".exe", ".cmd", ".bat", ".ps1"]
    } else {
        &[""]
    };
    for dir in std::env::split_paths(path_var) {
        for ext in exts {
            let candidate = dir.join(format!("{name}{ext}"));
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

// ─── External-command plumbing ───────────────────────────────────────────────

/// Run an external tool by its already-resolved executable path, capturing
/// output. The path comes from [`find_on_path`], so on Windows it is the actual
/// shim (`claude.cmd` etc.). `std` since 1.77 launches `.cmd`/`.bat` safely
/// (re-inserting a correctly-escaped `cmd /C` internally), so every argument —
/// including the MCP path — is passed as a true argv entry, immune to cmd.exe's
/// `^`/`%`/`&` re-parsing. Invoking the path directly (rather than the bare name
/// through `cmd /C`) is therefore both simpler and safer.
fn run_tool(bin: &Path, args: &[&str]) -> io::Result<std::process::Output> {
    Command::new(bin).args(args).output()
}

fn stderr_snippet(out: &std::process::Output) -> String {
    let s = String::from_utf8_lossy(&out.stderr);
    let trimmed = s.trim();
    if trimmed.is_empty() {
        format!("exit code {}", out.status.code().unwrap_or(-1))
    } else {
        trimmed.lines().next().unwrap_or(trimmed).to_owned()
    }
}

// ─── Outcome constructors ────────────────────────────────────────────────────

fn outcome(key: &'static str, status: Status, detail: impl Into<String>) -> ToolOutcome {
    ToolOutcome { key, status, detail: detail.into() }
}

fn not_installed(key: &'static str) -> ToolOutcome {
    outcome(key, Status::NotInstalled, "not installed (skipped)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn upsert_creates_top_key_and_entry() {
        let entry = json!({ "command": "/bin/mempalace-mcp" });
        let (merged, changed) = upsert_mcp_entry(None, "mcpServers", "mempalace", &entry);
        assert!(changed);
        assert_eq!(merged["mcpServers"]["mempalace"]["command"], "/bin/mempalace-mcp");
    }

    #[test]
    fn upsert_is_idempotent_for_same_entry() {
        let entry = json!({ "command": "/bin/mempalace-mcp" });
        let (first, _) = upsert_mcp_entry(None, "mcpServers", "mempalace", &entry);
        let (_, changed) = upsert_mcp_entry(Some(first), "mcpServers", "mempalace", &entry);
        assert!(!changed, "re-applying the same entry must report no change");
    }

    #[test]
    fn upsert_preserves_other_keys_and_servers() {
        let existing = json!({
            "theme": "dark",
            "mcpServers": { "other": { "command": "/x" } }
        });
        let entry = json!({ "command": "/bin/mempalace-mcp" });
        let (merged, changed) = upsert_mcp_entry(Some(existing), "mcpServers", "mempalace", &entry);
        assert!(changed);
        assert_eq!(merged["theme"], "dark", "unrelated top-level keys preserved");
        assert_eq!(merged["mcpServers"]["other"]["command"], "/x", "sibling server preserved");
        assert_eq!(merged["mcpServers"]["mempalace"]["command"], "/bin/mempalace-mcp");
    }

    #[test]
    fn upsert_replaces_non_object_top_key() {
        let existing = json!({ "mcpServers": "oops-a-string" });
        let entry = json!({ "command": "/c" });
        let (merged, changed) = upsert_mcp_entry(Some(existing), "mcpServers", "mempalace", &entry);
        assert!(changed);
        assert_eq!(merged["mcpServers"]["mempalace"]["command"], "/c");
    }

    #[test]
    fn upsert_detects_changed_command() {
        let (first, _) = upsert_mcp_entry(None, "mcpServers", "mempalace", &json!({ "command": "/old" }));
        let (merged, changed) =
            upsert_mcp_entry(Some(first), "mcpServers", "mempalace", &json!({ "command": "/new" }));
        assert!(changed, "a different command must count as a change");
        assert_eq!(merged["mcpServers"]["mempalace"]["command"], "/new");
    }

    #[test]
    fn lookup_finds_executable_unix_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir.path().join("mytool");
        std::fs::write(&exe, b"#!/bin/sh\n").unwrap();
        let path_var = OsString::from(dir.path());
        let found = lookup_in_paths("mytool", &path_var, false);
        assert_eq!(found.as_deref(), Some(exe.as_path()));
        assert!(lookup_in_paths("absent", &path_var, false).is_none());
    }

    #[test]
    fn lookup_tries_windows_extensions() {
        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("gemini.cmd");
        std::fs::write(&shim, b"@echo off\n").unwrap();
        let path_var = OsString::from(dir.path());
        // Bare name has no match on unix-semantics, but windows-semantics finds the .cmd shim.
        assert!(lookup_in_paths("gemini", &path_var, false).is_none());
        assert_eq!(lookup_in_paths("gemini", &path_var, true).as_deref(), Some(shim.as_path()));
    }

    #[test]
    fn apply_json_writes_then_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("opencode.json");

        let first = apply_json("opencode", &path, "mcp", json!({ "type": "local", "command": ["/m"] }), false);
        assert_eq!(first.status, Status::Configured);
        assert!(path.is_file(), "config file created (with parent dirs)");

        let again = apply_json("opencode", &path, "mcp", json!({ "type": "local", "command": ["/m"] }), false);
        assert_eq!(again.status, Status::AlreadyConfigured, "second run is a no-op");
    }

    #[test]
    fn apply_json_dry_run_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mcp-config.json");
        let res = apply_json("copilot", &path, "mcpServers", json!({ "command": "/m" }), true);
        assert_eq!(res.status, Status::WouldConfigure);
        assert!(!path.exists(), "dry run must not create the file");
    }

    #[test]
    fn apply_json_refuses_to_clobber_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.json");
        std::fs::write(&path, b"{ not json").unwrap();
        let res = apply_json("copilot", &path, "mcpServers", json!({ "command": "/m" }), false);
        assert_eq!(res.status, Status::Failed);
        // File left untouched.
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "{ not json");
    }

    #[test]
    fn gemini_list_matches_name_as_token_not_substring() {
        // Exact name (with trailing detail) and decorated lines match.
        assert!(list_contains_server("mempalace  stdio  /home/u/.mempalace/bin/mempalace-mcp", "mempalace"));
        assert!(list_contains_server("  * mempalace: connected", "mempalace"));
        assert!(list_contains_server("other\nmempalace ok", "mempalace"));
        // A different, longer name must NOT match.
        assert!(!list_contains_server("mempalace-old  stdio", "mempalace"));
        assert!(!list_contains_server("my-mempalace  stdio", "mempalace"));
        // A line that only contains the binary path (different server) must NOT match.
        assert!(!list_contains_server("github: /home/u/.mempalace/bin/mempalace-mcp", "mempalace"));
        assert!(!list_contains_server("", "mempalace"));
    }

    #[test]
    fn antigravity_marker_ignores_shared_gemini_config_dir() {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path();
        // Gemini-CLI-only home: ~/.gemini/config exists, but no antigravity-cli/.
        std::fs::create_dir_all(home.join(".gemini").join("config")).unwrap();
        assert!(!antigravity_home_marker(home), "shared ~/.gemini/config must NOT imply antigravity");
        // A real Antigravity home has ~/.gemini/antigravity-cli/.
        std::fs::create_dir_all(home.join(".gemini").join("antigravity-cli")).unwrap();
        assert!(antigravity_home_marker(home));
    }

    #[test]
    fn jules_is_never_configured() {
        // Whether or not `jules` is on PATH, it must never be Configured.
        let res = handle_tool("jules", "/home/u/.mempalace/bin/mempalace-mcp", false);
        assert!(matches!(res.status, Status::Unsupported | Status::NotInstalled));
    }

    #[test]
    fn empty_mcp_path_fails_cleanly() {
        let res = handle_tool("claude", "", false);
        assert_eq!(res.status, Status::Failed);
    }

    #[test]
    fn report_renders_all_statuses() {
        let report = SetupReport {
            mcp_path: PathBuf::from("/home/u/.mempalace/bin/mempalace-mcp"),
            mcp_binary_present: false,
            dry_run: true,
            outcomes: vec![
                outcome("claude", Status::WouldConfigure, "would run: ..."),
                outcome("jules", Status::Unsupported, "cloud-only"),
            ],
        };
        let text = report.render();
        assert!(text.contains("MemPalace setup"));
        assert!(text.contains("binary not found"));
        assert!(text.contains("claude"));
        assert!(text.contains("dry run"));
    }
}
