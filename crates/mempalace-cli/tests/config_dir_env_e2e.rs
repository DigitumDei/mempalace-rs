//! Proves `MEMPALACE_CONFIG_DIR` reaches the compiled `mempalace-cli` binary
//! in production.
//!
//! `mempalace-cli`'s `CliContext::production()` always passes `None` for the
//! config base-dir override, so the only way the override reaches the
//! binary is through `mempalace_config::resolve_paths` reading the env var
//! itself. These tests spawn the real binary (not the in-process `run_cli`
//! harness other CLI tests use, which always injects an explicit override
//! and therefore can't observe this) to prove the wiring actually works
//! end to end.
//!
//! `status` is used because it never touches the embedding provider (no
//! model cache or network access needed): it just resolves config, checks
//! whether a palace exists at the resolved path, and reports the path in
//! its error message when it doesn't. That resolved path is exactly what
//! this test needs to observe.

#![allow(clippy::unwrap_used)]

use std::process::Command;

use tempfile::TempDir;

#[test]
fn compiled_binary_status_resolves_default_palace_under_config_dir_override() {
    let tempdir = TempDir::new().unwrap();
    let unused_home = tempdir.path().join("unused-home");
    std::fs::create_dir_all(&unused_home).unwrap();
    let config_dir = tempdir.path().join("config-dir");
    std::fs::create_dir_all(&config_dir).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_mempalace-cli"))
        .arg("status")
        .env("HOME", &unused_home)
        .env("MEMPALACE_CONFIG_DIR", &config_dir)
        .env_remove("MEMPALACE_PALACE_PATH")
        .env_remove("MEMPAL_PALACE_PATH")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    let expected_palace = config_dir.join("palace");
    assert!(
        stderr.contains(&expected_palace.display().to_string()),
        "expected stderr to reference the config-dir-derived default palace \
         path {expected_palace:?}, got: {stderr}"
    );
    assert!(!unused_home.join(".mempalace").exists());
}

// The "MEMPALACE_CONFIG_DIR unset preserves current behaviour" half of the
// contract is covered in `mempalace-config`'s own unit tests
// (`config_dir_env_var_unset_preserves_default_base_dir`), which check
// against the exact same `dirs::home_dir()`-based resolution the binary
// uses, rather than asserting on `$HOME` here — home-directory resolution is
// platform-dependent enough that hard-coding it in an end-to-end test would
// make this file flaky on non-Unix runners.

#[test]
fn compiled_binary_palace_path_env_var_wins_over_config_dir_default_palace() {
    let tempdir = TempDir::new().unwrap();
    let unused_home = tempdir.path().join("unused-home");
    std::fs::create_dir_all(&unused_home).unwrap();
    let config_dir = tempdir.path().join("config-dir");
    std::fs::create_dir_all(&config_dir).unwrap();
    let explicit_palace = tempdir.path().join("explicit-palace");

    let output = Command::new(env!("CARGO_BIN_EXE_mempalace-cli"))
        .arg("status")
        .env("HOME", &unused_home)
        .env("MEMPALACE_CONFIG_DIR", &config_dir)
        .env("MEMPALACE_PALACE_PATH", &explicit_palace)
        .env_remove("MEMPAL_PALACE_PATH")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&explicit_palace.display().to_string()),
        "expected MEMPALACE_PALACE_PATH to win for the reported palace path, got: {stderr}"
    );
    assert!(!stderr.contains(&config_dir.join("palace").display().to_string()));
}
