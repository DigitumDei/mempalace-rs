//! One-time, offline migration from MemPalace. Never rewrites stored memories.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{Value, json};

const MARKER: &str = "mempalace-migration.json";

fn invalid(message: impl ToString) -> io::Error {
    io::Error::other(message.to_string())
}

/// Refuse to copy a database while either generation of the server may be writing.
pub fn check_stopped() -> io::Result<()> {
    #[cfg(windows)]
    let output = Command::new("tasklist").args(["/FO", "CSV", "/NH"]).output()?;
    #[cfg(not(windows))]
    let output = Command::new("ps").args(["-A", "-o", "pid=,comm="]).output()?;
    if !output.status.success() {
        return Err(invalid("Cannot inspect running processes; migration aborted"));
    }
    let own_pid = std::process::id().to_string();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        #[cfg(windows)]
        let (name, pid) = {
            let columns: Vec<_> = line.trim_matches('"').split("\",\"").collect();
            (columns.first().copied().unwrap_or(""), columns.get(1).copied().unwrap_or(""))
        };
        #[cfg(not(windows))]
        let (name, pid) = {
            let mut columns = line.split_whitespace();
            let pid = columns.next().unwrap_or("");
            (columns.next().unwrap_or(""), pid)
        };
        let name = name.rsplit(['/', '\\']).next().unwrap_or(name).to_ascii_lowercase();
        if pid != own_pid
            && matches!(
                name.as_str(),
                "mempalace"
                    | "mempalace.exe"
                    | "mempalace-mcp"
                    | "mempalace-mcp.exe"
                    | "mempalace-cli"
                    | "mempalace-cli.exe"
                    | "agentpalace"
                    | "agentpalace.exe"
            )
        {
            return Err(invalid(format!(
                "Stop {name} (PID {pid}) and its MCP host/service before migrating, then rerun the installer"
            )));
        }
    }
    Ok(())
}

/// Migrate the default home and existing supported MCP registrations.
pub fn run(
    from: &Path,
    to: &Path,
    home: &Path,
    binary: &Path,
    dry_run: bool,
) -> io::Result<String> {
    let from = absolute(from)?;
    let to = absolute(to)?;
    if from == to || from.starts_with(&to) || to.starts_with(&from) {
        return Err(invalid("Migration source and destination must be separate directories"));
    }
    let physical_from = physical_path(&from)?;
    let physical_to = physical_path(&to)?;
    if physical_from.starts_with(&physical_to) || physical_to.starts_with(&physical_from) {
        return Err(invalid("Migration directories overlap through a filesystem alias"));
    }
    let binary = absolute(binary)?;
    let physical_binary = physical_path(&binary)?;
    if physical_binary.starts_with(&physical_from)
        || physical_binary.starts_with(physical_path(&backup_path(&from))?)
    {
        return Err(invalid("Install the new executable outside the legacy home and its backup"));
    }
    // Read and validate every client document before changing the palace.
    let edits = client_edits(home, &from, &to, &binary)?;
    validate_data(&from, &to)?;
    let cache = if dirs::home_dir().as_deref() == Some(home) {
        dirs::cache_dir().map(|root| (root.join("mempalace"), root.join("agentpalace")))
    } else {
        None
    };
    if dirs::home_dir().as_deref() == Some(home) {
        if let Some(hf_home) = std::env::var_os("HF_HOME") {
            let hf_home = PathBuf::from(hf_home);
            for (old, new) in
                std::iter::once((&from, &to)).chain(cache.iter().map(|(old, new)| (old, new)))
            {
                if old.exists() {
                    if let Ok(relative) = hf_home.strip_prefix(old) {
                        return Err(invalid(format!(
                            "HF_HOME points inside a home being moved. Set HF_HOME to {} before retrying so future processes use the migrated cache",
                            new.join(relative).display()
                        )));
                    }
                }
            }
        }
    }
    if let Some((old, new)) = &cache {
        validate_data(old, new)?;
    }
    if dry_run {
        return Ok(format!(
            "Migration preview: {} -> {}; {} client configuration(s) to update. No files changed.\n",
            from.display(),
            to.display(),
            edits.len()
        ));
    }
    if let Some((old, new)) = &cache {
        migrate_data(old, new)?;
    }
    migrate_data(&from, &to)?;
    for (path, text) in &edits {
        backup_and_write(path, text)?;
    }
    Ok(format!(
        "Migration complete: {}; {} client configuration(s) updated. Original files retained as .pre-agentpalace backups. Restart your MCP hosts after installation.\n",
        to.display(),
        edits.len()
    ))
}

fn absolute(path: &Path) -> io::Result<PathBuf> {
    let path =
        if path.is_absolute() { path.to_path_buf() } else { std::env::current_dir()?.join(path) };
    if path.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
        return Err(invalid("Migration paths must not contain '..'"));
    }
    if fs::symlink_metadata(&path).is_ok_and(|m| m.file_type().is_symlink()) {
        return Err(invalid(format!("Migration root is a symlink: {}", path.display())));
    }
    Ok(path)
}

fn backup_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".pre-agentpalace");
    PathBuf::from(name)
}

// Resolve existing ancestors too: a destination beneath a junction/symlink
// must not put the staging directory inside the source being copied.
fn physical_path(path: &Path) -> io::Result<PathBuf> {
    if path.exists() {
        return fs::canonicalize(path);
    }
    let parent = path.parent().ok_or_else(|| invalid("Cannot resolve migration path"))?;
    let name = path.file_name().ok_or_else(|| invalid("Migration path has no file name"))?;
    Ok(physical_path(parent)?.join(name))
}

fn matching_marker(from: &Path, to: &Path) -> bool {
    fs::read(to.join(MARKER))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .is_some_and(|v| v == json!({"version": 1, "source": from, "destination": to}))
}

fn validate_data(from: &Path, to: &Path) -> io::Result<()> {
    absolute(from)?;
    absolute(to)?;
    if !from.exists() {
        return Ok(());
    }
    if !from.is_dir() {
        return Err(invalid("Migration source is not a directory"));
    }
    let config = from.join("config.json");
    if config.is_file() {
        let _: agentpalace_config::ConfigFileV1 = serde_json::from_slice(&fs::read(config)?)?;
    }
    if to.exists() && !matching_marker(from, to) {
        return Err(invalid(format!(
            "Both {} and {} exist. Refusing to merge palaces; resolve the destination explicitly before retrying",
            from.display(),
            to.display()
        )));
    }
    if backup_path(from).exists() {
        return Err(invalid(
            "Source and its migration backup both exist; refusing to overwrite either",
        ));
    }
    Ok(())
}

fn migrate_data(from: &Path, to: &Path) -> io::Result<()> {
    validate_data(from, to)?;
    if !from.exists() {
        return Ok(());
    }
    if !to.exists() {
        let parent = to.parent().ok_or_else(|| invalid("Destination has no parent"))?;
        fs::create_dir_all(parent)?;
        let staged = tempfile::Builder::new().prefix(".agentpalace-migrate-").tempdir_in(parent)?;
        copy_tree(from, staged.path(), true)?;
        let config = staged.path().join("config.json");
        if config.is_file() || staged.path().join("palace").is_dir() {
            let mut value: Value = if config.is_file() {
                serde_json::from_slice(&fs::read(&config)?)?
            } else {
                json!({})
            };
            value
                .as_object_mut()
                .ok_or_else(|| invalid("Global config is not an object"))?
                .entry("collection_name")
                .or_insert_with(|| json!("mempalace_drawers"));
            if let Some(path) = value.get_mut("palace_path") {
                remap_path_value(path, from, to);
            }
            if let Some(path) = value.pointer_mut("/server/token_file") {
                remap_path_value(path, from, to);
            }
            if let Some(remotes) =
                value.pointer_mut("/federation/remotes").and_then(Value::as_array_mut)
            {
                for remote in remotes {
                    if let Some(suffix) = remote
                        .get("token_env")
                        .and_then(Value::as_str)
                        .and_then(|s| s.strip_prefix("MEMPALACE_"))
                    {
                        remote["token_env"] = json!(format!("AGENTPALACE_{suffix}"));
                    }
                }
            }
            // collection names and every other persisted identifier remain untouched.
            fs::write(&config, serde_json::to_vec_pretty(&value)?)?;
        }
        let identity = staged.path().join("identity.txt");
        if identity.is_file() {
            let text = fs::read_to_string(&identity)?;
            fs::write(&identity, migrate_instructions(&text))?;
        }
        fs::write(
            staged.path().join(MARKER),
            serde_json::to_vec_pretty(&json!({"version":1,"source":from,"destination":to}))?,
        )?;
        // The complete copy becomes visible in one rename. If interrupted after
        // publication, the marker lets the next run finish retiring the source.
        fs::rename(staged.path(), to)?;
    }
    fs::rename(from, backup_path(from))?;
    Ok(())
}

fn copy_tree(from: &Path, to: &Path, root: bool) -> io::Result<()> {
    fs::set_permissions(to, fs::metadata(from)?.permissions())?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        if root && entry.file_name() == "bin" {
            continue;
        }
        let kind = entry.file_type()?;
        let target = to.join(entry.file_name());
        if kind.is_symlink() {
            return Err(invalid(format!(
                "Refusing to follow migration symlink {}. Keep custom storage outside the default home and configure its path explicitly",
                entry.path().display()
            )));
        } else if kind.is_dir() {
            fs::create_dir(&target)?;
            copy_tree(&entry.path(), &target, false)?;
        } else if kind.is_file() {
            fs::copy(entry.path(), &target)?;
        } else {
            return Err(invalid(format!(
                "Unsupported file in migration: {}",
                entry.path().display()
            )));
        }
    }
    Ok(())
}

fn remap_path_value(value: &mut Value, from: &Path, to: &Path) {
    if let Some(text) = value.as_str() {
        *value = json!(remap_path(text, from, to));
    }
}

fn remap_path(text: &str, from: &Path, to: &Path) -> String {
    let expanded = if text == "~/.mempalace"
        || text.starts_with("~/.mempalace/")
        || text.starts_with("~/.mempalace\\")
    {
        dirs::home_dir()
            .unwrap_or_default()
            .join(".mempalace")
            .join(text.trim_start_matches("~/.mempalace").trim_start_matches(['/', '\\']))
    } else {
        PathBuf::from(text)
    };
    match expanded.strip_prefix(from) {
        Ok(relative) if relative.as_os_str().is_empty() => to.to_string_lossy().into_owned(),
        Ok(relative) => to.join(relative).to_string_lossy().into_owned(),
        Err(_) => text.to_owned(),
    }
}

fn legacy_command(command: &str) -> bool {
    matches!(
        command.rsplit(['/', '\\']).next(),
        Some(
            "mempalace"
                | "mempalace.exe"
                | "mempalace-mcp"
                | "mempalace-mcp.exe"
                | "mempalace-cli"
                | "mempalace-cli.exe"
        )
    )
}

fn migrate_entry(entry: &mut Value, from: &Path, to: &Path, binary: &Path) -> io::Result<()> {
    let object = entry.as_object_mut().ok_or_else(|| invalid("MCP entry is not an object"))?;
    if let Some(command) = object.get_mut("command") {
        if let Some(array) = command.as_array_mut() {
            let old = array
                .first()
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("Empty MCP command"))?;
            if !legacy_command(old) {
                return Err(invalid(
                    "Legacy MCP entry uses a custom launcher; migrate its command manually",
                ));
            }
            let split = old.contains("-mcp");
            array[0] = json!(binary);
            if split {
                array.splice(1..1, [json!("serve"), json!("--stdio")]);
            }
            for argument in array.iter_mut().skip(1) {
                remap_path_value(argument, from, to);
            }
        } else {
            let old = command.as_str().ok_or_else(|| invalid("Invalid MCP command"))?;
            if !legacy_command(old) {
                return Err(invalid(
                    "Legacy MCP entry uses a custom launcher; migrate its command manually",
                ));
            }
            let split = old.contains("-mcp");
            *command = json!(binary);
            let args = object.entry("args").or_insert_with(|| json!([]));
            let args = args.as_array_mut().ok_or_else(|| invalid("MCP args is not an array"))?;
            if split {
                args.splice(0..0, [json!("serve"), json!("--stdio")]);
            }
            for argument in args {
                remap_path_value(argument, from, to);
            }
        }
    } else if !object.contains_key("url") {
        return Err(invalid("Legacy MCP entry has neither command nor URL"));
    }
    for env_key in ["env", "environment"] {
        if let Some(env) = object.get_mut(env_key).and_then(Value::as_object_mut) {
            let keys: Vec<_> = env.keys().cloned().collect();
            for key in keys {
                if let Some(suffix) = key
                    .strip_prefix("MEMPALACE_")
                    .or_else(|| (key == "MEMPAL_PALACE_PATH").then_some("PALACE_PATH"))
                {
                    let new = format!("AGENTPALACE_{suffix}");
                    if env.contains_key(&new) {
                        return Err(invalid(format!(
                            "Both {key} and {new} exist in MCP environment"
                        )));
                    }
                    let mut value = env.remove(&key).expect("key collected from environment");
                    if suffix.ends_with("_DIR") || suffix.ends_with("_PATH") {
                        remap_path_value(&mut value, from, to);
                    }
                    env.insert(new, value);
                }
            }
            if let Some(path) = env.get_mut("HF_HOME") {
                remap_path_value(path, from, to);
            }
        }
    }
    if object.contains_key("command") {
        let env_key = if object["command"].is_array() { "environment" } else { "env" };
        let env = object
            .entry(env_key)
            .or_insert_with(|| json!({}))
            .as_object_mut()
            .ok_or_else(|| invalid("MCP environment is not an object"))?;
        env.entry("AGENTPALACE_CONFIG_DIR").or_insert_with(|| json!(to));
    }
    for key in ["enabled_tools", "disabled_tools", "autoApprove", "alwaysAllow"] {
        if let Some(names) = object.get_mut(key).and_then(Value::as_array_mut) {
            for name in names {
                if let Some(suffix) = name.as_str().and_then(|s| s.strip_prefix("mempalace_")) {
                    *name = json!(format!("agentpalace_{suffix}"));
                }
            }
        }
    }
    Ok(())
}

fn client_edits(
    home: &Path,
    from: &Path,
    to: &Path,
    binary: &Path,
) -> io::Result<Vec<(PathBuf, String)>> {
    let mut edits = Vec::new();
    for (relative, key) in [
        (".claude.json", "mcpServers"),
        (".gemini/settings.json", "mcpServers"),
        (".copilot/mcp-config.json", "mcpServers"),
        (".gemini/antigravity/mcp_config.json", "mcpServers"),
        (".gemini/antigravity-cli/mcp_config.json", "mcpServers"),
        (".config/opencode/opencode.json", "mcp"),
    ] {
        let path = if relative == ".config/opencode/opencode.json"
            && dirs::home_dir().as_deref() == Some(home)
        {
            std::env::var_os("XDG_CONFIG_HOME")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".config"))
                .join("opencode/opencode.json")
        } else {
            home.join(relative)
        };
        if !path.exists() {
            continue;
        }
        let mut doc: Value = serde_json::from_slice(&fs::read(&path)?)?;
        if let Some(servers) = doc.get_mut(key).and_then(Value::as_object_mut) {
            if let Some(mut entry) = servers.get("mempalace").cloned() {
                if servers.contains_key("agentpalace") {
                    return Err(invalid(format!("Both MCP names exist in {}", path.display())));
                }
                migrate_entry(&mut entry, from, to, binary)?;
                servers.remove("mempalace");
                servers.insert("agentpalace".into(), entry);
                edits.push((path, serde_json::to_string_pretty(&doc)? + "\n"));
            }
        }
    }
    let codex = if dirs::home_dir().as_deref() == Some(home) {
        std::env::var_os("CODEX_HOME").map(PathBuf::from).unwrap_or_else(|| home.join(".codex"))
    } else {
        home.join(".codex")
    };
    for path in [codex.join("AGENTS.md"), home.join(".claude/CLAUDE.md")] {
        if path.is_file() {
            let source = fs::read_to_string(&path)?;
            let updated = migrate_instructions(&source);
            if source != updated {
                edits.push((path, updated));
            }
        }
    }
    let path = codex.join("config.toml");
    if path.exists() {
        let source = fs::read_to_string(&path)?;
        let parsed: toml::Value = toml::from_str(&source).map_err(invalid)?;
        let mut doc = source.parse::<toml_edit::DocumentMut>().map_err(invalid)?;
        if let Some(servers) =
            doc.get_mut("mcp_servers").and_then(toml_edit::Item::as_table_like_mut)
        {
            if servers.contains_key("mempalace") {
                if servers.contains_key("agentpalace") {
                    return Err(invalid("Both MCP names exist in Codex configuration"));
                }
                let mut value = serde_json::to_value(&parsed["mcp_servers"]["mempalace"])?;
                migrate_entry(&mut value, from, to, binary)?;
                let wrapped = json!({"agentpalace":value});
                let updated = toml::to_string(&wrapped)
                    .map_err(invalid)?
                    .parse::<toml_edit::DocumentMut>()
                    .map_err(invalid)?;
                servers.remove("mempalace");
                servers.insert("agentpalace", updated["agentpalace"].clone());
                edits.push((path, doc.to_string()));
            }
        }
    }
    Ok(edits)
}

// Active instructions must call the new tool names after restarting. This
// intentionally does not touch diary entries, drawer contents or transcripts.
fn migrate_instructions(text: &str) -> String {
    fn prefix(text: &str, old: &str, new: &str) -> String {
        let mut result = String::new();
        let mut last = 0;
        for (offset, _) in text.match_indices(old) {
            if text[..offset].chars().next_back().is_some_and(|c| c.is_alphanumeric() || c == '_') {
                continue;
            }
            result.push_str(&text[last..offset]);
            result.push_str(new);
            last = offset + old.len();
        }
        result.push_str(&text[last..]);
        result
    }
    prefix(&prefix(text, "mempalace_", "agentpalace_"), "MEMPALACE_", "AGENTPALACE_")
        .replace("`mempalace ", "`agentpalace ")
}

fn backup_and_write(path: &Path, text: &str) -> io::Result<()> {
    let backup = backup_path(path);
    if !backup.exists() {
        fs::copy(path, &backup)?;
    }
    let parent = path.parent().ok_or_else(|| invalid("Config has no parent"))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent)?;
    fs::set_permissions(temp.path(), fs::metadata(path)?.permissions())?;
    use std::io::Write;
    temp.write_all(text.as_bytes())?;
    temp.as_file().sync_all()?;
    temp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn aliased_destination_cannot_stage_inside_source() {
        let home = tempfile::tempdir().unwrap();
        let from = home.path().join("old");
        fs::create_dir(&from).unwrap();
        let alias = home.path().join("alias");
        std::os::unix::fs::symlink(&from, &alias).unwrap();
        assert!(
            run(&from, &alias.join("new"), home.path(), Path::new("/bin/agentpalace"), false)
                .is_err()
        );
        assert!(from.exists());
        assert!(!from.join("new").exists());
    }

    #[test]
    fn opencode_migration_preserves_environment_and_sets_custom_home() {
        let mut entry = json!({"type":"local", "command":["/old/mempalace","serve","--stdio","--palace","/custom/palace"],
            "environment":{"TOKEN":"secret","MEMPALACE_EMBEDDING_PROFILE":"low_cpu"}});
        migrate_entry(
            &mut entry,
            Path::new("/old-home"),
            Path::new("/new-home"),
            Path::new("/bin/agentpalace"),
        )
        .unwrap();
        assert_eq!(entry["environment"]["TOKEN"], "secret");
        assert_eq!(entry["environment"]["AGENTPALACE_CONFIG_DIR"], json!(Path::new("/new-home")));
        assert_eq!(entry["environment"]["AGENTPALACE_EMBEDDING_PROFILE"], "low_cpu");
        assert_eq!(entry["command"][4], "/custom/palace");
        assert!(entry.get("env").is_none());
    }

    #[test]
    fn instruction_migration_does_not_rename_durable_wing_ids() {
        assert_eq!(
            migrate_instructions("Call mempalace_search in wing_mempalace_rs."),
            "Call agentpalace_search in wing_mempalace_rs."
        );
    }

    #[tokio::test]
    async fn migrated_palace_reopens_real_lance_and_sqlite_data() {
        use agentpalace_core::{DrawerRecord, EmbeddingProfile};
        use agentpalace_storage::{DrawerStore, IngestManifestStore, StorageEngine};
        let home = tempfile::tempdir().unwrap();
        let from = home.path().join(".mempalace");
        let to = home.path().join(".agentpalace");
        let record: DrawerRecord = serde_json::from_value(json!({
            "id":"drawer_wing_mempalace_rs_general_123", "wing":"wing_mempalace_rs", "room":"general",
            "hall":null,"date":null,"source_file":"historic.txt","chunk_index":0,"ingest_mode":"fixtures",
            "extract_mode":null,"added_by":"codex","filed_at":"2026-09-11T12:00:00Z",
            "importance":null,"emotional_weight":null,"weight":null,
            "content":"MemPalace historical memory", "content_hash":"fixed",
            "embedding":vec![0.1_f32;EmbeddingProfile::Balanced.metadata().dimensions]
        })).unwrap();
        let engine =
            StorageEngine::open(from.join("palace"), EmbeddingProfile::Balanced).await.unwrap();
        engine
            .drawer_store()
            .put_drawers(
                std::slice::from_ref(&record),
                agentpalace_storage::DuplicateStrategy::Overwrite,
            )
            .await
            .unwrap();
        let pending = engine
            .operational_store()
            .create_pending_run(
                "projects",
                "projects:old-id:file",
                &[],
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
        engine
            .operational_store()
            .mark_run_committed(
                pending.id,
                "projects:old-id:file",
                "file",
                "hash",
                1,
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
        drop(engine);
        run(&from, &to, home.path(), &to.join("bin/agentpalace"), false).unwrap();
        let reopened =
            StorageEngine::open(to.join("palace"), EmbeddingProfile::Balanced).await.unwrap();
        assert_eq!(reopened.drawer_store().get_drawer(&record.id).await.unwrap(), Some(record));
        assert_eq!(
            reopened
                .operational_store()
                .get_ingested_file("projects:old-id:file")
                .unwrap()
                .unwrap()
                .content_hash,
            "hash"
        );
    }

    #[test]
    fn migration_preserves_data_ids_secrets_and_custom_paths_and_is_repeatable() {
        let home = tempfile::tempdir().unwrap();
        let from = home.path().join(".mempalace");
        let to = home.path().join(".agentpalace");
        fs::create_dir_all(from.join("palace")).unwrap();
        fs::write(from.join("palace/memory"), b"mempalace_kg_query historical content").unwrap();
        let config = json!({"palace_path":from.join("palace"),"collection_name":"mempalace_drawers","server":{"token_file":"/custom/token.json"}});
        fs::write(from.join("config.json"), serde_json::to_vec(&config).unwrap()).unwrap();
        fs::write(from.join("projects.json"), b"{\"projects\":{\"github.com/old/mempalace\":{}}}")
            .unwrap();
        let old = json!({"mcpServers":{"mempalace":{"command":"/old/mempalace","args":["serve","--stdio","--palace","/custom/palace"],"env":{"MEMPALACE_CONFIG_DIR":from,"MY_TOKEN":"untouched"},"enabled_tools":["mempalace_search"]}},"other":true});
        let client = home.path().join(".claude.json");
        fs::write(&client, serde_json::to_vec(&old).unwrap()).unwrap();
        run(&from, &to, home.path(), &to.join("bin/agentpalace"), true).unwrap();
        assert!(!to.exists());
        run(&from, &to, home.path(), &to.join("bin/agentpalace"), false).unwrap();
        assert!(!from.exists());
        assert_eq!(
            fs::read(to.join("palace/memory")).unwrap(),
            b"mempalace_kg_query historical content"
        );
        assert_eq!(
            fs::read(to.join("projects.json")).unwrap(),
            fs::read(backup_path(&from).join("projects.json")).unwrap()
        );
        let migrated: Value =
            serde_json::from_slice(&fs::read(to.join("config.json")).unwrap()).unwrap();
        assert_eq!(migrated["palace_path"], json!(to.join("palace")));
        assert_eq!(migrated["server"]["token_file"], "/custom/token.json");
        let updated = fs::read(&client).unwrap();
        let migrated: Value = serde_json::from_slice(&updated).unwrap();
        assert!(migrated["mcpServers"].get("mempalace").is_none());
        assert_eq!(
            migrated["mcpServers"]["agentpalace"]["env"]["AGENTPALACE_CONFIG_DIR"],
            json!(to)
        );
        assert_eq!(migrated["mcpServers"]["agentpalace"]["env"]["MY_TOKEN"], "untouched");
        assert_eq!(migrated["mcpServers"]["agentpalace"]["args"][3], "/custom/palace");
        assert_eq!(migrated["mcpServers"]["agentpalace"]["enabled_tools"][0], "agentpalace_search");
        run(&from, &to, home.path(), &to.join("bin/agentpalace"), false).unwrap();
        assert_eq!(updated, fs::read(&client).unwrap());
    }

    #[test]
    fn conflicts_and_invalid_clients_leave_source_untouched() {
        let home = tempfile::tempdir().unwrap();
        let from = home.path().join(".mempalace");
        let to = home.path().join(".agentpalace");
        fs::create_dir(&from).unwrap();
        fs::create_dir(&to).unwrap();
        assert!(run(&from, &to, home.path(), Path::new("/bin/agentpalace"), false).is_err());
        assert!(from.exists());
        fs::remove_dir(&to).unwrap();
        fs::write(home.path().join(".claude.json"), "{broken").unwrap();
        assert!(run(&from, &to, home.path(), Path::new("/bin/agentpalace"), false).is_err());
        assert!(from.exists());
        assert!(!to.exists());
    }

    #[test]
    fn publication_interruption_resumes_retirement() {
        let home = tempfile::tempdir().unwrap();
        let from = home.path().join("old");
        let to = home.path().join("new");
        fs::create_dir(&from).unwrap();
        fs::create_dir(&to).unwrap();
        fs::write(
            to.join(MARKER),
            serde_json::to_vec(&json!({"version":1,"source":from,"destination":to})).unwrap(),
        )
        .unwrap();
        migrate_data(&from, &to).unwrap();
        assert!(backup_path(&from).is_dir());
        migrate_data(&from, &to).unwrap();
    }

    #[test]
    fn codex_migration_preserves_custom_environment_and_other_tables() {
        let home = tempfile::tempdir().unwrap();
        fs::create_dir(home.path().join(".codex")).unwrap();
        let path = home.path().join(".codex/config.toml");
        fs::write(&path,"model = 'chosen'\n[mcp_servers.mempalace]\ncommand = '/old/mempalace-mcp'\nargs = ['--palace', '/custom']\nstartup_timeout_sec = 120\n[mcp_servers.mempalace.env]\nMEMPALACE_EMBEDDING_PROFILE = 'low_cpu'\nTOKEN = 'secret'\n[mcp_servers.other]\nurl = 'https://example.test'\n").unwrap();
        let from = home.path().join(".mempalace");
        let to = home.path().join(".agentpalace");
        run(&from, &to, home.path(), Path::new("/new/agentpalace"), false).unwrap();
        let parsed: toml::Value = toml::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed["model"].as_str(), Some("chosen"));
        assert_eq!(parsed["mcp_servers"]["agentpalace"]["env"]["TOKEN"].as_str(), Some("secret"));
        assert_eq!(parsed["mcp_servers"]["agentpalace"]["args"][0].as_str(), Some("serve"));
        assert_eq!(parsed["mcp_servers"]["agentpalace"]["args"][2].as_str(), Some("--palace"));
        assert_eq!(parsed["mcp_servers"]["other"]["url"].as_str(), Some("https://example.test"));
    }
}
