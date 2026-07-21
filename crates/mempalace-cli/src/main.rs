#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use mempalace_config::{
    ConfigFileV1, ConfigLoader, MempalaceConfig, ProjectConfig, ProjectRegistryEntryV1,
    ProjectRoomConfig, ResolvedPaths, RouteMode, RouteQuery, WriteTarget, build_runtime,
    resolve_route,
};
use mempalace_core::{EmbeddingProfile, RoomId, SearchQuery, WingId};
use mempalace_embeddings::{
    DeterministicStubProvider, EmbeddingProvider, FastembedProvider, FastembedProviderConfig,
    env_flag, log_startup_validation,
};
use mempalace_server::{TokenRegistry, build_router};
use mempalace_federation::{IngestBatchRequest, IngestBatchResponse};
use mempalace_ingest::{
    ConversationExtractMode, ConversationIngestRequest, IngestError, IngestSummary,
    ProjectIngestRequest, derive_project_id, ingest_conversations, ingest_project_with_config,
    prepare_project_batch_with_config, project_root_relative,
};
use mempalace_remote::{RemoteApi, RemoteClient, RemoteEndpoint, RemoteError};
use mempalace_search::{Layer1Config, SearchRuntime, SearchRuntimePolicy, WakeUpRequest};
use mempalace_storage::{DrawerFilter, DrawerStore, StorageEngine, StorageLayout};
use serde_yaml::Mapping;
use tracing_subscriber::{EnvFilter, fmt};

mod setup;

const DEFERRED_COMMAND_DOC: &str = "docs/rust-phase-plans/Phase09-Deferred-Commands.md";

const INIT_HEADER_WIDTH: usize = 55;
const STATUS_HEADER_WIDTH: usize = 55;
const SEARCH_HEADER_WIDTH: usize = 60;
const WAKE_UP_SEPARATOR_WIDTH: usize = 50;

const SKIP_DIRS: &[&str] = &[
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    "dist",
    "build",
    ".next",
    "coverage",
    ".mempalace",
];

const PROJECT_FILE_EXTENSIONS: &[&str] = &[
    "txt", "md", "py", "js", "ts", "jsx", "tsx", "json", "yaml", "yml", "html", "css", "java",
    "go", "rs", "rb", "sh", "csv", "sql", "toml",
];

const FOLDER_ROOM_MAP: &[(&str, &str)] = &[
    ("frontend", "frontend"),
    ("front_end", "frontend"),
    ("client", "frontend"),
    ("ui", "frontend"),
    ("views", "frontend"),
    ("components", "frontend"),
    ("pages", "frontend"),
    ("backend", "backend"),
    ("back_end", "backend"),
    ("server", "backend"),
    ("api", "backend"),
    ("routes", "backend"),
    ("services", "backend"),
    ("controllers", "backend"),
    ("models", "backend"),
    ("database", "backend"),
    ("db", "backend"),
    ("docs", "documentation"),
    ("doc", "documentation"),
    ("documentation", "documentation"),
    ("wiki", "documentation"),
    ("readme", "documentation"),
    ("notes", "documentation"),
    ("design", "design"),
    ("designs", "design"),
    ("mockups", "design"),
    ("wireframes", "design"),
    ("assets", "design"),
    ("storyboard", "design"),
    ("costs", "costs"),
    ("cost", "costs"),
    ("budget", "costs"),
    ("finance", "costs"),
    ("financial", "costs"),
    ("pricing", "costs"),
    ("invoices", "costs"),
    ("accounting", "costs"),
    ("meetings", "meetings"),
    ("meeting", "meetings"),
    ("calls", "meetings"),
    ("meeting_notes", "meetings"),
    ("standup", "meetings"),
    ("minutes", "meetings"),
    ("team", "team"),
    ("staff", "team"),
    ("hr", "team"),
    ("hiring", "team"),
    ("employees", "team"),
    ("people", "team"),
    ("research", "research"),
    ("references", "research"),
    ("reading", "research"),
    ("papers", "research"),
    ("planning", "planning"),
    ("roadmap", "planning"),
    ("strategy", "planning"),
    ("specs", "planning"),
    ("requirements", "planning"),
    ("tests", "testing"),
    ("test", "testing"),
    ("testing", "testing"),
    ("qa", "testing"),
    ("scripts", "scripts"),
    ("tools", "scripts"),
    ("utils", "scripts"),
    ("config", "configuration"),
    ("configs", "configuration"),
    ("settings", "configuration"),
    ("infrastructure", "configuration"),
    ("infra", "configuration"),
    ("deploy", "configuration"),
];

fn main() {
    init_tracing("info");

    let result = run_cli_with_validation_factory(
        env::args_os().skip(1),
        &CliContext::production(),
        fastembed_provider,
        fastembed_validation_provider,
    );

    match result {
        Ok(output) => {
            if !output.stdout.is_empty() {
                print!("{}", output.stdout);
            }
            if !output.stderr.is_empty() {
                eprint!("{}", output.stderr);
            }
            std::process::exit(output.exit_code);
        }
        Err(error) => {
            eprint!("{error}");
            std::process::exit(2);
        }
    }
}

fn init_tracing(default_filter: &str) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    let _ = fmt().with_env_filter(filter).with_target(false).try_init();
}

#[derive(Debug, Parser)]
#[command(
    name = "mempalace-cli",
    about = "MemPalace — Give your AI a memory. No API key required.",
    version
)]
struct Cli {
    #[arg(
        long,
        global = true,
        value_name = "PATH",
        help = "Where the palace lives (default: from ~/.mempalace/config.json or ~/.mempalace/palace)"
    )]
    palace: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Detect rooms from your folder structure.
    Init {
        dir: PathBuf,
        #[arg(long, help = "Auto-accept detected rooms")]
        yes: bool,
        #[arg(long = "repo-config", help = "Also write a portable repository-local mempalace.yaml")]
        repo_config: bool,
        #[arg(long = "project-id", alias = "project", help = "Explicit stable project ID for repositories without a usable origin")]
        project_id: Option<String>,
    },
    /// Mine files into the palace.
    Mine {
        dir: PathBuf,
        #[arg(long, value_enum, default_value_t = CliMode::Projects)]
        mode: CliMode,
        #[arg(long)]
        wing: Option<String>,
        #[arg(long = "project-id", alias = "project", help = "Explicit project registry ID")]
        project_id: Option<String>,
        #[arg(long, default_value = "mempalace")]
        agent: String,
        #[arg(long, default_value_t = 0)]
        limit: usize,
        #[arg(long = "dry-run")]
        dry_run: bool,
        #[arg(long = "reindex", help = "Re-process all discovered files even if their content hash is unchanged (migration path from content rows to locator rows)")]
        reindex: bool,
        #[arg(long, value_enum, default_value_t = CliExtractMode::Exchange)]
        extract: CliExtractMode,
        #[arg(long, help = "Mine only files changed vs the merge-base with the default branch (local branch-delta mining)")]
        branch: bool,
        #[arg(
            long = "batch-size",
            value_name = "N",
            help = "Largest batch to process at once; lower it to bound peak memory/CPU on low-spec machines. Local mine: chunks embedded per batch (default: a file's chunks together). Remote mine: files per request (default: 64). 0 or omitted keeps the default."
        )]
        batch_size: Option<usize>,
    },
    /// Inspect and manage centralized project declarations.
    Project {
        #[command(subcommand)]
        command: ProjectCommands,
    },
    /// Find anything, exact words.
    Search {
        query: String,
        #[arg(long)]
        wing: Option<String>,
        #[arg(long)]
        room: Option<String>,
        #[arg(long = "results", default_value_t = 5)]
        results: usize,
    },
    /// Show what's been filed.
    Status,
    /// Show L0 + L1 wake-up context.
    #[command(name = "wake-up")]
    WakeUp {
        #[arg(long)]
        wing: Option<String>,
    },
    /// Wire the mempalace MCP server into your installed AI coding tools.
    Setup {
        #[arg(long = "dry-run", help = "Preview what would change without writing anything or running tool commands")]
        dry_run: bool,
        #[arg(long = "mcp-path", help = "Path to the mempalace-mcp binary (default: ~/.mempalace/bin/mempalace-mcp[.exe])")]
        mcp_path: Option<PathBuf>,
        #[arg(long, value_delimiter = ',', help = "Limit to specific tools (comma-separated): claude,codex,gemini,opencode,copilot,antigravity,jules")]
        tools: Option<Vec<String>>,
    },
    /// Deferred in Rust Phase 9. See the linked decision record.
    Split {
        dir: PathBuf,
        #[arg(long = "output-dir")]
        output_dir: Option<PathBuf>,
        #[arg(long = "dry-run")]
        dry_run: bool,
        #[arg(long = "min-sessions", default_value_t = 2)]
        min_sessions: usize,
    },
    /// Deferred in Rust Phase 9. See the linked decision record.
    Compress {
        #[arg(long)]
        wing: Option<String>,
        #[arg(long = "dry-run")]
        dry_run: bool,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Run the federation HTTP server over this palace.
    Serve {
        #[arg(long, help = "Bind address, e.g. 127.0.0.1:8765 (default: from config)")]
        bind: Option<SocketAddr>,
        #[arg(long = "token-file", help = "Path to the bearer token JSON file (default: ~/.mempalace/server_tokens.json)")]
        token_file: Option<PathBuf>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum CliMode {
    Projects,
    Convos,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum CliExtractMode {
    Exchange,
    General,
}

#[derive(Debug, Clone)]
struct CliContext {
    config_base_dir: Option<PathBuf>,
}

impl CliContext {
    fn production() -> Self {
        Self { config_base_dir: None }
    }

    #[cfg(test)]
    fn for_tests(config_base_dir: PathBuf) -> Self {
        Self { config_base_dir: Some(config_base_dir) }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CliOutput {
    exit_code: i32,
    stdout: String,
    stderr: String,
}

impl CliOutput {
    fn success(stdout: impl Into<String>) -> Self {
        Self { exit_code: 0, stdout: stdout.into(), stderr: String::new() }
    }

    fn failure(exit_code: i32, stderr: impl Into<String>) -> Self {
        Self { exit_code, stdout: String::new(), stderr: stderr.into() }
    }
}

/// Combine a local success with a best-effort remote replication result.
///
/// The local write determines the command's exit status. If a future remote
/// path returns a failure output instead of encoding its status in stdout, its
/// stderr still needs to be surfaced to the user without turning the local
/// success into a command failure.
fn combine_dual_write_outputs(local: CliOutput, remote: CliOutput) -> CliOutput {
    let remote_text = if remote.exit_code == 0 || remote.stderr.trim().is_empty() {
        remote.stdout
    } else {
        remote.stderr
    };
    CliOutput::success(format!("{}\n{}", local.stdout.trim(), remote_text.trim()))
}

fn run_cli<I, T, F, P>(
    args: I,
    context: &CliContext,
    provider_factory: F,
) -> Result<CliOutput, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>> + Copy,
    P: EmbeddingProvider + Send + Sync + 'static,
{
    run_cli_with_validation_factory(args, context, provider_factory, provider_factory)
}

fn run_cli_with_validation_factory<I, T, F, P, G, Q>(
    args: I,
    context: &CliContext,
    provider_factory: F,
    validation_provider_factory: G,
) -> Result<CliOutput, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>>,
    P: EmbeddingProvider + Send + Sync + 'static,
    G: Fn(EmbeddingProfile, PathBuf) -> Result<Q, Box<dyn std::error::Error>>,
    Q: EmbeddingProvider,
{
    let argv = std::iter::once(std::ffi::OsString::from("mempalace-cli"))
        .chain(args.into_iter().map(Into::into))
        .collect::<Vec<_>>();
    let cli = match Cli::try_parse_from(argv) {
        Ok(cli) => cli,
        Err(error) if error.kind() == clap::error::ErrorKind::DisplayHelp => {
            return Ok(CliOutput::success(error.to_string()));
        }
        Err(error) if error.kind() == clap::error::ErrorKind::DisplayVersion => {
            return Ok(CliOutput::success(error.to_string()));
        }
        Err(error) => return Err(error),
    };

    if cli.command.is_none() {
        return Ok(CliOutput::success(render_help()));
    }

    execute(cli, context, provider_factory, validation_provider_factory)
}

fn render_help() -> String {
    let mut command = Cli::command();
    let mut buffer = Vec::new();
    if command.write_long_help(&mut buffer).is_err() {
        return "mempalace-cli\n".to_owned();
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

fn execute<F, P, G, Q>(
    cli: Cli,
    context: &CliContext,
    provider_factory: F,
    validation_provider_factory: G,
) -> Result<CliOutput, clap::Error>
where
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>>,
    P: EmbeddingProvider + Send + Sync + 'static,
    G: Fn(EmbeddingProfile, PathBuf) -> Result<Q, Box<dyn std::error::Error>>,
    Q: EmbeddingProvider,
{
    let Some(command) = cli.command else {
        return Ok(CliOutput::success(render_help()));
    };
    match command {
        Commands::Init { dir, yes, repo_config, project_id } => execute_init(
            &dir,
            yes,
            repo_config,
            project_id.as_deref(),
            cli.palace.as_deref(),
            context,
            validation_provider_factory,
        ),
        Commands::Mine { dir, mode, wing, project_id, agent, limit, dry_run, reindex, extract, branch, batch_size } => {
            execute_mine(
                &dir,
                mode,
                wing,
                project_id,
                agent,
                limit,
                dry_run,
                reindex,
                extract,
                branch,
                batch_size,
                cli.palace.as_deref(),
                context,
                provider_factory,
            )
        }
        Commands::Project { command } => execute_project_command(command, context),
        Commands::Search { query, wing, room, results } => execute_search(
            &query,
            wing,
            room,
            results,
            cli.palace.as_deref(),
            context,
            provider_factory,
        ),
        Commands::Status => execute_status(cli.palace.as_deref(), context),
        Commands::WakeUp { wing } => {
            execute_wake_up(wing, cli.palace.as_deref(), context, provider_factory)
        }
        Commands::Setup { dry_run, mcp_path, tools } => {
            let report = setup::run_setup(&setup::SetupOptions { mcp_path, dry_run, only: tools });
            Ok(CliOutput::success(report.render()))
        }
        Commands::Split { .. } => Ok(deferred_command("split")),
        Commands::Compress { .. } => Ok(deferred_command("compress")),
        Commands::Serve { bind, token_file } => execute_serve(
            bind,
            token_file,
            cli.palace.as_deref(),
            context,
            provider_factory,
        ),
    }
}

fn execute_init<F, P>(
    dir: &Path,
    yes: bool,
    repo_config: bool,
    explicit_project_id: Option<&str>,
    palace_override: Option<&Path>,
    context: &CliContext,
    provider_factory: F,
) -> Result<CliOutput, clap::Error>
where
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>>,
    P: EmbeddingProvider,
{
    let project_dir = dir.canonicalize().map_err(|source| {
        clap::Error::raw(
            clap::error::ErrorKind::Io,
            format!("failed to access project directory `{}`: {source}", dir.display()),
        )
    })?;

    let file_count = count_project_files(&project_dir).map_err(io_error)?;
    let detection = detect_rooms(&project_dir).map_err(io_error)?;
    let config_path = project_dir.join("mempalace.yaml");
    let legacy_config_path = project_dir.join("mempal.yaml");
    let existing_config = config_path.exists() || legacy_config_path.exists();

    if existing_config && repo_config && !yes {
        return Ok(CliOutput::failure(
            1,
            format!(
                "{} already exists; re-run `mempalace-cli init {}` with `--yes` to overwrite it\n",
                config_path.display(),
                project_dir.display()
            ),
        ));
    }

    let runtime_paths = init_runtime_config(palace_override, context).map_err(config_error)?;
    let config = load_runtime_config(palace_override, context).map_err(config_error)?;
    let provider = provider_factory(config.embedding_profile, default_embedding_cache_dir())
        .map_err(provider_error)?;
    let validation = provider.startup_validation().map_err(provider_error)?;
    log_startup_validation(&validation);

    let project_config = if existing_config {
        ConfigLoader::load_project_config(&project_dir).map_err(config_error)?
    } else {
        let derived_wing = wing_name_for_dir(&project_dir);
        let candidate_project_id =
            derive_project_id(&project_dir, &derived_wing, explicit_project_id);
        ConfigLoader::resolve_project_config(
            &project_dir,
            context.config_base_dir.as_deref(),
            Some(&candidate_project_id),
            &derived_wing,
            detection.rooms.clone(),
        )
        .map_err(config_error)?
    };
    let derived_project_id = derive_project_id(&project_dir, &project_config.wing, explicit_project_id);
    let project_id = if explicit_project_id.filter(|id| !id.trim().is_empty()).is_some() {
        derived_project_id
    } else {
        ConfigLoader::find_project_id(
            context.config_base_dir.as_deref(),
            &project_dir,
            Some(&derived_project_id),
        )
        .map_err(config_error)?
        .unwrap_or(derived_project_id)
    };
    if explicit_project_id.filter(|id| !id.trim().is_empty()).is_none()
        && project_id.starts_with("wing:")
        && ConfigLoader::load_project_registry(context.config_base_dir.as_deref())
            .map_err(config_error)?
            .projects
            .contains_key(&project_id)
        && ConfigLoader::find_project_id(
            context.config_base_dir.as_deref(),
            &project_dir,
            None,
        )
        .map_err(config_error)?
        .is_none()
    {
        return Ok(CliOutput::failure(
            1,
            format!(
                "project `{project_id}` is ambiguous for a checkout without a Git origin; pass --project-id\n"
            ),
        ));
    }
    let project_root = project_root_relative(&project_dir);
    let registry_path = ConfigLoader::register_project(
        context.config_base_dir.as_deref(),
        &project_id,
        ProjectRegistryEntryV1 {
            project_root,
            ..ProjectRegistryEntryV1::from(project_config.clone())
        },
        Some(&project_dir),
    )
    .map_err(config_error)?;

    if repo_config {
        write_project_config(&config_path, &project_dir, &project_config, true)
            .map_err(io_error)?;
    }

    let mut lines = vec![
        format!("\n{}", "=".repeat(INIT_HEADER_WIDTH)),
        "  MemPalace Init — Local setup".to_owned(),
        "=".repeat(INIT_HEADER_WIDTH),
        String::new(),
        format!("  WING: {}", project_config.wing),
        format!("  ({} files found, rooms detected from {})", file_count, detection.source),
        String::new(),
    ];

    for room in &detection.rooms {
        lines.push(format!("    ROOM: {}", room.name));
        lines
            .push(format!("          {}", room.description.as_deref().unwrap_or("No description")));
    }

    lines.extend([
        String::new(),
        format!("{}", "─".repeat(INIT_HEADER_WIDTH)),
        format!("  Project registry: {}", registry_path.display()),
        if repo_config {
            format!("  Repository config: {}", config_path.display())
        } else {
            "  Repository config: not written (use --repo-config to opt in)".to_owned()
        },
        format!("  Palace path: {}", config.palace_path.display()),
        format!("  Startup validation: {}", validation.status),
        format!("  Global config: {}", runtime_paths.config_file.display()),
        "  Next step:".to_owned(),
        format!("    mempalace-cli mine {}", project_dir.display()),
        format!("\n{}\n", "=".repeat(INIT_HEADER_WIDTH)),
    ]);

    Ok(CliOutput::success(lines.join("\n")))
}

fn execute_project_command(
    command: ProjectCommands,
    context: &CliContext,
) -> Result<CliOutput, clap::Error> {
    match command {
        ProjectCommands::Register { dir, wing, project_id: explicit_project_id, repo_config } => {
            let project_dir = dir.canonicalize().map_err(|source| {
                clap::Error::raw(
                    clap::error::ErrorKind::Io,
                    format!("failed to access project directory `{}`: {source}", dir.display()),
                )
            })?;
            let detection = detect_rooms(&project_dir).map_err(io_error)?;
            let mut project_config = if project_dir.join("mempalace.yaml").exists()
                || project_dir.join("mempal.yaml").exists()
            {
                ConfigLoader::load_project_config(&project_dir).map_err(config_error)?
            } else {
                let derived_wing = wing.clone().unwrap_or_else(|| wing_name_for_dir(&project_dir));
                let candidate_project_id =
                    derive_project_id(&project_dir, &derived_wing, explicit_project_id.as_deref());
                ConfigLoader::resolve_project_config(
                    &project_dir,
                    context.config_base_dir.as_deref(),
                    Some(&candidate_project_id),
                    &derived_wing,
                    detection.rooms,
                )
                .map_err(config_error)?
            };
            if let Some(wing) = wing {
                project_config.wing = wing;
            }
            let derived_project_id = derive_project_id(
                &project_dir,
                &project_config.wing,
                explicit_project_id.as_deref(),
            );
            let project_id = if explicit_project_id
                .as_deref()
                .filter(|id| !id.trim().is_empty())
                .is_some()
            {
                derived_project_id
            } else {
                ConfigLoader::find_project_id(
                    context.config_base_dir.as_deref(),
                    &project_dir,
                    Some(&derived_project_id),
                )
                .map_err(config_error)?
                .unwrap_or(derived_project_id)
            };
            if explicit_project_id
                .as_deref()
                .filter(|id| !id.trim().is_empty())
                .is_none()
                && project_id.starts_with("wing:")
                && ConfigLoader::load_project_registry(context.config_base_dir.as_deref())
                    .map_err(config_error)?
                    .projects
                    .contains_key(&project_id)
                && ConfigLoader::find_project_id(
                    context.config_base_dir.as_deref(),
                    &project_dir,
                    None,
                )
                .map_err(config_error)?
                .is_none()
            {
                return Ok(CliOutput::failure(
                    1,
                    format!(
                        "project `{project_id}` is ambiguous for a checkout without a Git origin; pass --project-id\n"
                    ),
                ));
            }
            let registry_path = ConfigLoader::register_project(
                context.config_base_dir.as_deref(),
                &project_id,
                ProjectRegistryEntryV1 {
                    project_root: project_root_relative(&project_dir),
                    ..ProjectRegistryEntryV1::from(project_config.clone())
                },
                Some(&project_dir),
            )
            .map_err(config_error)?;
            if repo_config {
                write_project_config(
                    &project_dir.join("mempalace.yaml"),
                    &project_dir,
                    &project_config,
                    true,
                )
                .map_err(io_error)?;
            }

            let mut lines = vec![
                format!("Project registered: {project_id}"),
                format!("  Wing: {}", project_config.wing),
                format!("  Registry: {}", registry_path.display()),
            ];
            if repo_config {
                lines.push(format!(
                    "  Repository config: {}",
                    project_dir.join("mempalace.yaml").display()
                ));
            }
            Ok(CliOutput::success(format!("{}\n", lines.join("\n"))))
        }
        ProjectCommands::Show { dir } => {
            let project_dir = dir.canonicalize().map_err(|source| {
                clap::Error::raw(
                    clap::error::ErrorKind::Io,
                    format!("failed to access project directory `{}`: {source}", dir.display()),
                )
            })?;
            let derived_wing = wing_name_for_dir(&project_dir);
            let project_id = derive_project_id(&project_dir, &derived_wing, None);
            let project_config = ConfigLoader::resolve_project_config(
                &project_dir,
                context.config_base_dir.as_deref(),
                Some(&project_id),
                &derived_wing,
                Vec::new(),
            )
            .map_err(config_error)?;
            let project_id = ConfigLoader::find_project_id(
                context.config_base_dir.as_deref(),
                &project_dir,
                Some(&project_id),
            )
            .map_err(config_error)?
            .unwrap_or(project_id);
            let mut lines = vec![
                format!("Project: {project_id}"),
                format!("  Wing: {}", project_config.wing),
                "  Rooms:".to_owned(),
            ];
            lines.extend(project_config.rooms.iter().map(|room| {
                format!("    - {}", room.name)
            }));
            if let Some(routing) = project_config.routing {
                lines.push(format!("  Routing: {:?}", routing.mode));
            }
            Ok(CliOutput::success(format!("{}\n", lines.join("\n"))))
        }
        ProjectCommands::List => {
            let registry = ConfigLoader::load_project_registry(context.config_base_dir.as_deref())
                .map_err(config_error)?;
            let mut lines = vec![format!("Projects: {}", registry.projects.len())];
            for (project_id, entry) in registry.projects {
                lines.push(format!("  {project_id} -> {}", entry.wing));
            }
            Ok(CliOutput::success(format!("{}\n", lines.join("\n"))))
        }
        ProjectCommands::Remove { project_id } => {
            let (registry_path, removed) =
                ConfigLoader::remove_project(context.config_base_dir.as_deref(), &project_id)
                    .map_err(config_error)?;
            if !removed {
                return Ok(CliOutput::failure(
                    1,
                    format!("project `{project_id}` is not registered\n"),
                ));
            }
            Ok(CliOutput::success(format!(
                "Removed project {project_id} from {}\n",
                registry_path.display()
            )))
        }
        ProjectCommands::Export { project_id, dir, repo_config } => {
            if !repo_config {
                return Ok(CliOutput::failure(
                    1,
                    "project export requires --repo-config\n".to_owned(),
                ));
            }
            let registry = ConfigLoader::load_project_registry(context.config_base_dir.as_deref())
                .map_err(config_error)?;
            let entry = registry.projects.get(&project_id).ok_or_else(|| {
                clap::Error::raw(
                    clap::error::ErrorKind::InvalidValue,
                    format!("project `{project_id}` is not registered"),
                )
            })?;
            let target_dir = match dir {
                Some(dir) => dir.canonicalize().map_err(|source| {
                    clap::Error::raw(
                        clap::error::ErrorKind::Io,
                        format!("failed to access project directory `{}`: {source}", dir.display()),
                    )
                })?,
                None => entry.checkouts.first().cloned().ok_or_else(|| {
                    clap::Error::raw(
                        clap::error::ErrorKind::InvalidValue,
                        format!("project `{project_id}` has no checkout alias; pass --dir"),
                    )
                })?,
            };
            let project_config = ProjectConfig {
                wing: entry.wing.clone(),
                rooms: entry.rooms.clone(),
                routing: entry.routing.clone(),
            };
            let path = target_dir.join("mempalace.yaml");
            write_project_config(&path, &target_dir, &project_config, true).map_err(io_error)?;
            Ok(CliOutput::success(format!(
                "Exported project {project_id} to {}\n",
                path.display()
            )))
        }
}
}

fn execute_mine<F, P>(
    dir: &Path,
    mode: CliMode,
    wing: Option<String>,
    explicit_project_id: Option<String>,
    agent: String,
    limit: usize,
    dry_run: bool,
    reindex: bool,
    extract: CliExtractMode,
    branch: bool,
    batch_size: Option<usize>,
    palace_override: Option<&Path>,
    context: &CliContext,
    provider_factory: F,
) -> Result<CliOutput, clap::Error>
where
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>>,
    P: EmbeddingProvider,
{
    let source_dir = dir.canonicalize().map_err(|source| {
        clap::Error::raw(
            clap::error::ErrorKind::Io,
            format!("failed to access source directory `{}`: {source}", dir.display()),
        )
    })?;

    let config = load_runtime_config(palace_override, context).map_err(config_error)?;
    let runtime = build_runtime(&config).map_err(runtime_error)?;

    // ── Routing decision (projects mode only) ────────────────────────────────
    if mode == CliMode::Projects {
        let derived_wing = wing.clone().unwrap_or_else(|| wing_name_for_dir(&source_dir));
        let candidate_project_id = derive_project_id(
            &source_dir,
            &derived_wing,
            explicit_project_id.as_deref(),
        );
        let project_config = if let Some(project_id) = explicit_project_id.as_deref() {
            match ConfigLoader::load_project_by_id(
                context.config_base_dir.as_deref(),
                project_id,
            )
            .map_err(config_error)? {
                Some(config) => config,
                None => {
                    return Ok(CliOutput::failure(
                        1,
                        format!("project `{project_id}` is not registered\n"),
                    ));
                }
            }
        } else {
            ConfigLoader::resolve_project_config(
                &source_dir,
                context.config_base_dir.as_deref(),
                Some(&candidate_project_id),
                &derived_wing,
                Vec::new(),
            )
            .map_err(config_error)?
        };
        let project_id = if let Some(project_id) = explicit_project_id.as_deref() {
            project_id.to_owned()
        } else {
            ConfigLoader::find_project_id(
                context.config_base_dir.as_deref(),
                &source_dir,
                Some(&candidate_project_id),
            )
            .map_err(config_error)?
            .unwrap_or_else(|| derive_project_id(&source_dir, &project_config.wing, None))
        };
        let wing_name = wing
            .clone()
            .unwrap_or_else(|| project_config.wing.clone());

        let project_routing = if wing
            .as_deref()
            .is_some_and(|override_wing| !wing_ids_equal(override_wing, &project_config.wing))
        {
            None
        } else {
            project_config.routing.as_ref()
        };
        let rule = resolve_route(
            &config.federation,
            project_routing,
            RouteQuery { wing: Some(&wing_name), room: None, source_file: None },
        );

        let use_remote = !branch
            && (rule.mode == RouteMode::Remote
                || (rule.mode == RouteMode::Combined
                    && rule.write == WriteTarget::Remote));
        let use_both = !branch
            && rule.mode == RouteMode::Combined
            && rule.write == WriteTarget::Both;

        if use_remote {
            return execute_remote_mine(
                &source_dir,
                wing,
                &agent,
                limit,
                dry_run,
                batch_size,
                &config,
                &runtime,
                &rule,
                &project_config,
                Some(&project_id),
                false,
            );
        }

        if use_both {
            let local_output = execute_local_project_mine(
                &source_dir,
                wing.clone(),
                agent.clone(),
                limit,
                dry_run,
                reindex,
                batch_size,
                branch,
                &config,
                &runtime,
                &provider_factory,
                &project_config,
                Some(&project_id),
            )?;
            let remote_output = execute_remote_mine(
                &source_dir,
                wing,
                &agent,
                limit,
                dry_run,
                batch_size,
                &config,
                &runtime,
                &rule,
                &project_config,
                Some(&project_id),
                true,
            )?;
            return Ok(combine_dual_write_outputs(local_output, remote_output));
        }

        // ── Local mine path continues below with the resolved declaration. ──
        return execute_local_project_mine(
            &source_dir,
            wing,
            agent,
            limit,
            dry_run,
            reindex,
            batch_size,
            branch,
            &config,
            &runtime,
            provider_factory,
            &project_config,
            Some(&project_id),
        );
    } else {
        // Convos mode: --branch is not supported; remote routing is not supported.
        if branch {
            return Ok(CliOutput::failure(
                1,
                "--branch requires --mode projects; branch-delta mining is not supported for conversations\n",
            ));
        }
        // Conversation mining is always local: routing rules are wing-based and
        // convo directories carry no project wing config to resolve against.
    }

    // ── Conversation mine path ──────────────────────────────────────────────
    let use_temp_storage = dry_run && !palace_exists(&config.palace_path);
    let dry_run_storage =
        if use_temp_storage { Some(tempfile::tempdir().map_err(io_error)?) } else { None };
    let storage_root =
        dry_run_storage.as_ref().map(|dir| dir.path()).unwrap_or(config.palace_path.as_path());
    let engine = runtime
        .block_on(StorageEngine::open(storage_root, config.embedding_profile))
        .map_err(storage_error)?;
    let mut provider = provider_factory(config.embedding_profile, default_embedding_cache_dir())
        .map_err(provider_error)?;

    let max_embed_batch_size = batch_size.filter(|&n| n > 0).or_else(|| {
        config
            .low_cpu
            .enabled
            .then(|| config.low_cpu.effective_ingest_batch_size())
    });

    let summary = runtime
        .block_on(ingest_conversations(
            &engine,
            &mut provider,
            &ConversationIngestRequest {
                convo_dir: source_dir.clone(),
                wing,
                agent,
                extract_mode: match extract {
                    CliExtractMode::Exchange => ConversationExtractMode::Exchange,
                    CliExtractMode::General => ConversationExtractMode::General,
                },
                limit: if limit == 0 { None } else { Some(limit) },
                dry_run,
                reindex,
                max_embed_batch_size,
            },
        ))
        .map_err(ingest_error)?;

    Ok(CliOutput::success(render_mine_summary(
        mode,
        &source_dir,
        &config.palace_path,
        dry_run,
        &summary,
    )))
}

#[derive(Debug, Subcommand)]
enum ProjectCommands {
    /// Register or update a project declaration.
    Register {
        dir: PathBuf,
        #[arg(long)]
        wing: Option<String>,
        #[arg(long = "project-id", alias = "project", help = "Explicit project registry ID")]
        project_id: Option<String>,
        #[arg(long = "repo-config")]
        repo_config: bool,
    },
    /// Show the declaration resolved for a checkout.
    Show { dir: PathBuf },
    /// List all centralized project declarations.
    List,
    /// Remove a declaration by its stable project ID.
    Remove { project_id: String },
    /// Export a centralized declaration to a repository-local compatibility file.
    Export {
        project_id: String,
        #[arg(long)]
        dir: Option<PathBuf>,
        #[arg(long = "repo-config")]
        repo_config: bool,
    },
}

#[allow(clippy::too_many_arguments)]
fn execute_local_project_mine<F, P>(
    source_dir: &Path,
    wing: Option<String>,
    agent: String,
    limit: usize,
    dry_run: bool,
    reindex: bool,
    batch_size: Option<usize>,
    branch: bool,
    config: &MempalaceConfig,
    runtime: &tokio::runtime::Runtime,
    provider_factory: F,
    project_config: &ProjectConfig,
    project_id: Option<&str>,
) -> Result<CliOutput, clap::Error>
where
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>>,
    P: EmbeddingProvider,
{
    let use_temp_storage = dry_run && !palace_exists(&config.palace_path);
    let dry_run_storage =
        if use_temp_storage { Some(tempfile::tempdir().map_err(io_error)?) } else { None };
    let storage_root =
        dry_run_storage.as_ref().map(|dir| dir.path()).unwrap_or(config.palace_path.as_path());
    let engine = runtime
        .block_on(StorageEngine::open(storage_root, config.embedding_profile))
        .map_err(storage_error)?;
    let mut provider = provider_factory(config.embedding_profile, default_embedding_cache_dir())
        .map_err(provider_error)?;
    let max_embed_batch_size = batch_size.filter(|&n| n > 0).or_else(|| {
        config
            .low_cpu
            .enabled
            .then(|| config.low_cpu.effective_ingest_batch_size())
    });

    let summary = runtime
        .block_on(ingest_project_with_config(
            &engine,
            &mut provider,
            &ProjectIngestRequest {
                project_dir: source_dir.to_path_buf(),
                wing,
                agent,
                limit: if limit == 0 { None } else { Some(limit) },
                dry_run,
                reindex,
                max_embed_batch_size,
                branch,
            },
            project_config,
            project_id,
        ))
        .map_err(|e| branch_delta_error(e, ingest_error))?;

    Ok(CliOutput::success(render_mine_summary(
        CliMode::Projects,
        source_dir,
        &config.palace_path,
        dry_run,
        &summary,
    )))
}

/// Map an [`IngestError`] to a clap error, giving a friendly message for
/// [`IngestError::BranchDeltaUnavailable`].
fn branch_delta_error(
    error: IngestError,
    fallback: impl Fn(IngestError) -> clap::Error,
) -> clap::Error {
    match error {
        IngestError::BranchDeltaUnavailable { ref reason } => clap::Error::raw(
            clap::error::ErrorKind::Io,
            format!(
                "--branch mining requires a git repository with a detectable default branch \
                 (main or master): {reason}\n\
                 Hint: run `git init` and commit at least once, or omit --branch for a full mine.\n"
            ),
        ),
        other => fallback(other),
    }
}

/// Maximum accumulated chunk-text bytes before flushing a remote batch (~4 MiB).
const REMOTE_BATCH_MAX_BYTES: usize = 4 * 1024 * 1024;
/// Maximum number of files per remote batch.
const REMOTE_BATCH_MAX_FILES: usize = 64;

#[allow(clippy::too_many_arguments)]
fn execute_remote_mine(
    source_dir: &Path,
    wing_override: Option<String>,
    agent: &str,
    limit: usize,
    dry_run: bool,
    batch_size: Option<usize>,
    config: &MempalaceConfig,
    runtime: &tokio::runtime::Runtime,
    rule: &mempalace_config::ResolvedRouteRule,
    project_config: &ProjectConfig,
    project_id: Option<&str>,
    dual_write: bool,
) -> Result<CliOutput, clap::Error> {
    // `--batch-size N` (N>0) caps files per remote request, letting low-spec
    // machines bound how much is held/serialized at once. The ~4 MiB byte cap
    // still applies as an independent guardrail against the server body limit.
    let max_files_per_batch = batch_size.filter(|&n| n > 0).unwrap_or(REMOTE_BATCH_MAX_FILES);
    // ── 1. Prepare the batch (no embedding, no storage) ─────────────────────
    let prepared = match prepare_project_batch_with_config(
        &ProjectIngestRequest {
        project_dir: source_dir.to_path_buf(),
        wing: wing_override.clone(),
        agent: agent.to_owned(),
        limit: if limit == 0 { None } else { Some(limit) },
        dry_run: false,
        reindex: false,
        max_embed_batch_size: None,
        branch: false,
        },
        project_config,
        project_id,
    ) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("failed to prepare project batch: {e}\n");
            return if dual_write {
                Ok(CliOutput::success(format!("  Remote replication: failed — {msg}")))
            } else {
                Ok(CliOutput::failure(1, msg))
            };
        }
    };

    // ── 2. Resolve the remote config entry ──────────────────────────────────
    let remote_name = rule.remote.as_deref().unwrap_or("remote");
    let resolved_remote =
        match config.federation.remotes.get(remote_name) {
            Some(r) => r,
            None => {
                let msg = format!(
                    "federation config error: route for wing '{}' refers to remote '{}', \
                     but no such remote is defined in config.federation.remotes\n",
                    prepared.wing, remote_name
                );
                let output = if dual_write {
                    format!("  Remote replication: failed — {msg}")
                } else {
                    format!("{msg}")
                };
                return if dual_write {
                    Ok(CliOutput::success(output))
                } else {
                    Ok(CliOutput::failure(1, output))
                };
            }
        };
    let remote_url = resolved_remote.url.clone();

    // ── 3. Non-default-branch warning text (computed early for dry-run) ─────
    let branch_warning = match (&prepared.current_branch, &prepared.default_branch) {
        (Some(cur), Some(def)) if cur != def => Some(format!(
            "warning: mining branch '{}' into the shared remote wing '{}' \
             (expected '{}')\n",
            cur, prepared.wing, def
        )),
        _ => None,
    };

    // Count total chunks for dry-run display.
    let total_chunks: usize = prepared.files.iter().map(|f| f.chunks.len()).sum();

    // ── 4. Dry-run: print plan and return ────────────────────────────────────
    if dry_run {
        let mut lines = vec![
            format!("\n{}", "=".repeat(SEARCH_HEADER_WIDTH)),
            "  Mine plan (dry run)".to_owned(),
            "=".repeat(SEARCH_HEADER_WIDTH),
            "  Mode: projects (remote)".to_owned(),
            format!("  Source: {}", source_dir.display()),
            format!("  Remote: {} ({})", remote_name, remote_url),
            format!("  Wing: {}", prepared.wing),
            format!("  Repo ID: {}", prepared.repo_id),
            format!(
                "  Commit: {}",
                prepared.commit_hash.as_deref().unwrap_or("<none>")
            ),
            format!("  Files to send: {}", prepared.files.len()),
            format!("  Total chunks: {total_chunks}"),
            format!("  Max files/batch: {max_files_per_batch}"),
        ];
        if let Some(ref warning) = branch_warning {
            lines.push(format!("  {}", warning.trim()));
        }
        lines.push(format!("{}\n", "=".repeat(SEARCH_HEADER_WIDTH)));
        return Ok(CliOutput::success(lines.join("\n")));
    }

    // ── 5. Build the remote client ───────────────────────────────────────────
    let endpoint = RemoteEndpoint {
        name: remote_name.to_owned(),
        base_url: remote_url.clone(),
        token: resolved_remote.token.clone(),
        timeout: resolved_remote.timeout,
    };
    let client = match RemoteClient::new(endpoint) {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("failed to build remote client for '{}': {e}\n", remote_name);
            let output = if dual_write {
                format!("  Remote replication: failed — {msg}")
            } else {
                format!("{msg}")
            };
            return if dual_write {
                Ok(CliOutput::success(output))
            } else {
                Ok(CliOutput::failure(1, output))
            };
        }
    };

    // ── 6. Capabilities check ────────────────────────────────────────────────
    // Call through the trait to avoid shadowing by the `info` field on RemoteClient.
    let api: &dyn RemoteApi = &client;
    let info_resp = match runtime.block_on(api.info()) {
        Ok(resp) => resp,
        Err(e) => {
            let msg = match e {
                RemoteError::Unreachable { ref message, .. } => format!(
                    "remote '{}' is unreachable at {}: {}",
                    remote_name, remote_url, message
                ),
                other => format!("remote '{}' info() failed: {other}", remote_name),
            };
            let output = if dual_write {
                format!(
                    "  Remote replication: failed — {msg}\n\
                     Note: the local mine completed successfully; remote replication was skipped.\n"
                )
            } else {
                format!(
                    "{msg}\n\
                     Note: writes do not fall back to local.\n"
                )
            };
            return if dual_write {
                Ok(CliOutput::success(output))
            } else {
                Ok(CliOutput::failure(1, output))
            };
        }
    };

    if !info_resp.capabilities.iter().any(|c| c == "ingest") {
        let msg = if dual_write {
            format!(
                "  Remote replication: skipped — remote '{}' does not support the ingest capability.\n\
                 Server capabilities: {}\n\
                 Note: the local mine completed successfully.\n",
                remote_name,
                info_resp.capabilities.join(", ")
            )
        } else {
            format!(
                "remote '{}' does not support the ingest capability; \
                 please upgrade the remote server.\n\
                 Server capabilities: {}\n",
                remote_name,
                info_resp.capabilities.join(", ")
            )
        };
        return if dual_write {
            Ok(CliOutput::success(msg))
        } else {
            Ok(CliOutput::failure(1, msg))
        };
    }

    // ── 7. Batch and send ────────────────────────────────────────────────────
    let wing = prepared.wing.clone();
    let repo_id = prepared.repo_id.clone();
    let commit_hash = prepared.commit_hash.clone();
    let agent_owned = agent.to_owned();

    // Split files into batches.
    let batches = build_remote_batches(prepared.files, max_files_per_batch);

    let mut ingested_count: usize = 0;
    let mut skipped_count: usize = 0;
    let mut failed_count: usize = 0;
    let mut total_drawers_written: usize = 0;
    let mut all_warnings: Vec<String> = Vec::new();
    let mut failed_files: Vec<(String, String)> = Vec::new();
    let mut batches_sent: usize = 0;

    for batch_files in batches {
        let req = IngestBatchRequest {
            wing: wing.clone(),
            repo_id: repo_id.clone(),
            agent: Some(agent_owned.clone()),
            commit_hash: commit_hash.clone(),
            files: batch_files,
        };

        let resp: IngestBatchResponse =
            match runtime.block_on(api.ingest_batch(req)) {
                Ok(resp) => resp,
                Err(e) => {
                    let msg = format!(
                        "remote '{}' transport error after {} batch(es): {e}",
                        remote_name, batches_sent
                    );
                    if dual_write && batches_sent > 0 {
                        // Some batches completed — render partial summary showing
                        // what was achieved before the transport interruption.
                        let partial = render_remote_mine_summary(
                            source_dir,
                            remote_name,
                            &remote_url,
                            &wing,
                            &repo_id,
                            ingested_count,
                            skipped_count,
                            failed_count,
                            total_drawers_written,
                            &all_warnings,
                            &failed_files,
                            branch_warning.as_deref(),
                            true,
                            true,
                        );
                        let with_error = format!(
                            "{}\n  {}\n  Note: remote replication was incomplete — {msg}.\n",
                            partial.trim(),
                            "─".repeat(SEARCH_HEADER_WIDTH),
                        );
                        return Ok(CliOutput::success(with_error));
                    }
                    let output = if dual_write {
                        format!(
                            "  Remote replication: failed — {msg}\n\
                             Note: the local mine completed successfully; remote replication was incomplete.\n"
                        )
                    } else {
                        format!(
                            "{msg}\n\
                             Note: writes do not fall back to local.\n"
                        )
                    };
                    return if dual_write {
                        Ok(CliOutput::success(output))
                    } else {
                        Ok(CliOutput::failure(1, output))
                    };
                }
            };

        batches_sent += 1;

        for file_result in resp.files {
            match file_result.status.as_str() {
                "ingested" => {
                    ingested_count += 1;
                    total_drawers_written += file_result.drawers_written;
                }
                "skipped_unchanged" => {
                    skipped_count += 1;
                }
                _ => {
                    failed_count += 1;
                    failed_files.push((
                        file_result.relative_path,
                        file_result.error.unwrap_or_default(),
                    ));
                }
            }
        }

        // Dedupe warnings.
        for w in resp.warnings {
            if !all_warnings.contains(&w) {
                all_warnings.push(w);
            }
        }
    }

    // ── 8. Render summary ────────────────────────────────────────────────────
    let output = render_remote_mine_summary(
        source_dir,
        remote_name,
        &remote_url,
        &wing,
        &repo_id,
        ingested_count,
        skipped_count,
        failed_count,
        total_drawers_written,
        &all_warnings,
        &failed_files,
        branch_warning.as_deref(),
        dual_write,
        false,
    );

    Ok(CliOutput::success(output))
}

/// Split a flat list of [`IngestFileDto`] into batches bounded by `max_files`
/// files (caller-supplied; from `--batch-size`, defaulting to
/// [`REMOTE_BATCH_MAX_FILES`]) and [`REMOTE_BATCH_MAX_BYTES`] of chunk text.
fn build_remote_batches(
    files: Vec<mempalace_federation::IngestFileDto>,
    max_files: usize,
) -> Vec<Vec<mempalace_federation::IngestFileDto>> {
    // Guard against a zero slipping through: a 0 cap would never flush on the
    // file count and silently disable that bound. Treat it as the default.
    let max_files = if max_files == 0 { REMOTE_BATCH_MAX_FILES } else { max_files };
    let mut batches: Vec<Vec<mempalace_federation::IngestFileDto>> = Vec::new();
    let mut current: Vec<mempalace_federation::IngestFileDto> = Vec::new();
    let mut current_bytes: usize = 0;

    for file in files {
        let file_bytes: usize = file.chunks.iter().map(|c| c.text.len()).sum();

        // An oversized single file goes alone in its own batch.
        let would_overflow_bytes =
            current_bytes + file_bytes > REMOTE_BATCH_MAX_BYTES && !current.is_empty();
        let would_overflow_files = current.len() >= max_files;

        if would_overflow_bytes || would_overflow_files {
            batches.push(std::mem::take(&mut current));
            current_bytes = 0;
        }

        current_bytes += file_bytes;
        current.push(file);
    }

    if !current.is_empty() {
        batches.push(current);
    }

    batches
}

#[allow(clippy::too_many_arguments)]
fn render_remote_mine_summary(
    source_dir: &Path,
    remote_name: &str,
    remote_url: &str,
    wing: &str,
    repo_id: &str,
    ingested_count: usize,
    skipped_count: usize,
    failed_count: usize,
    drawers_written: usize,
    warnings: &[String],
    failed_files: &[(String, String)],
    branch_warning: Option<&str>,
    dual_write: bool,
    replication_incomplete: bool,
) -> String {
    let mode_label = if dual_write { "replication (best-effort)" } else { "projects (remote)" };
    let mut lines = vec![
        format!("\n{}", "=".repeat(SEARCH_HEADER_WIDTH)),
        "  Mine complete".to_owned(),
        "=".repeat(SEARCH_HEADER_WIDTH),
        format!("  Mode: {mode_label}"),
        format!("  Source: {}", source_dir.display()),
        format!("  Remote: {remote_name} ({remote_url})"),
        format!("  Wing: {wing}"),
        format!("  Repo ID: {repo_id}"),
        format!("  Files ingested: {ingested_count}"),
        format!("  Files skipped unchanged: {skipped_count}"),
        format!("  Files failed: {failed_count}"),
        format!("  Drawers written: {drawers_written}"),
    ];

    if dual_write {
        if replication_incomplete {
            lines.insert(1, "  Remote replication: partial — transport interrupted".to_owned());
            lines.insert(2, String::new());
        } else if failed_count > 0 {
            lines.insert(1, "  Remote replication: partial — some files had errors".to_owned());
            lines.insert(2, String::new());
        } else {
            lines.insert(1, "  Remote replication: succeeded".to_owned());
            lines.insert(2, String::new());
        }
    }

    if !warnings.is_empty() {
        lines.push(String::new());
        lines.push("  Warnings:".to_owned());
        for w in warnings {
            lines.push(format!("    - {w}"));
        }
    }

    if let Some(bw) = branch_warning {
        lines.push(format!("  {}", bw.trim()));
    }

    if !failed_files.is_empty() {
        lines.push(String::new());
        lines.push("  Failed files:".to_owned());
        let cap = failed_files.len().min(10);
        for (path, err) in failed_files.iter().take(cap) {
            lines.push(format!("    {path}: {err}"));
        }
        if failed_files.len() > 10 {
            lines.push(format!("    ... and {} more", failed_files.len() - 10));
        }
    }

    lines.push(format!("{}\n", "=".repeat(SEARCH_HEADER_WIDTH)));
    lines.join("\n")
}

fn execute_search<F, P>(
    query: &str,
    wing: Option<String>,
    room: Option<String>,
    results: usize,
    palace_override: Option<&Path>,
    context: &CliContext,
    provider_factory: F,
) -> Result<CliOutput, clap::Error>
where
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>>,
    P: EmbeddingProvider,
{
    let config = load_runtime_config(palace_override, context).map_err(config_error)?;
    if !palace_exists(&config.palace_path) {
        return Ok(no_palace_error(&config.palace_path));
    }

    let runtime = build_runtime(&config).map_err(runtime_error)?;
    let engine = runtime
        .block_on(StorageEngine::open(&config.palace_path, config.embedding_profile))
        .map_err(storage_error)?;
    let provider = provider_factory(config.embedding_profile, default_embedding_cache_dir())
        .map_err(provider_error)?;
    let mut search = SearchRuntime::with_policy(
        provider,
        SearchRuntimePolicy { rerank_enabled: config.low_cpu.effective_rerank_enabled() },
    );

    let wing_id = wing.as_deref().map(WingId::normalized).transpose().map_err(id_error)?;
    let room_id = room.as_deref().map(RoomId::new).transpose().map_err(id_error)?;
    let rendered = runtime
        .block_on(search.search_text(
            engine.drawer_store(),
            &SearchQuery {
                text: query.to_owned(),
                wing: wing_id,
                room: room_id,
                limit: clamp_search_results(results, &config),
                profile: config.embedding_profile,
            },
        ))
        .map_err(search_error)?;

    Ok(CliOutput::success(rendered))
}

fn execute_status(
    palace_override: Option<&Path>,
    context: &CliContext,
) -> Result<CliOutput, clap::Error> {
    let config = load_runtime_config(palace_override, context).map_err(config_error)?;
    if !palace_exists(&config.palace_path) {
        return Ok(no_palace_error(&config.palace_path));
    }

    let runtime = build_runtime(&config).map_err(runtime_error)?;
    let engine = runtime
        .block_on(StorageEngine::open(&config.palace_path, config.embedding_profile))
        .map_err(storage_error)?;
    let drawers = runtime
        .block_on(engine.drawer_store().list_drawers(&DrawerFilter::default()))
        .map_err(storage_error)?;

    let mut wing_rooms = BTreeMap::<String, BTreeMap<String, usize>>::new();
    for drawer in drawers {
        *wing_rooms
            .entry(drawer.wing.to_string())
            .or_default()
            .entry(drawer.room.to_string())
            .or_default() += 1;
    }

    let mut lines = vec![
        format!("\n{}", "=".repeat(STATUS_HEADER_WIDTH)),
        format!(
            "  MemPalace Status — {} drawers",
            wing_rooms.values().map(|rooms| rooms.values().sum::<usize>()).sum::<usize>()
        ),
        "=".repeat(STATUS_HEADER_WIDTH),
        String::new(),
    ];

    for (wing, rooms) in wing_rooms {
        lines.push(format!("  WING: {wing}"));
        let mut room_counts = rooms.into_iter().collect::<Vec<_>>();
        room_counts.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
        for (room, count) in room_counts {
            lines.push(format!("    ROOM: {room:20} {count:5} drawers"));
        }
        lines.push(String::new());
    }

    lines.push("=".repeat(STATUS_HEADER_WIDTH));
    lines.push(String::new());
    Ok(CliOutput::success(lines.join("\n")))
}

fn execute_wake_up<F, P>(
    wing: Option<String>,
    palace_override: Option<&Path>,
    context: &CliContext,
    provider_factory: F,
) -> Result<CliOutput, clap::Error>
where
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>>,
    P: EmbeddingProvider,
{
    let config = load_runtime_config(palace_override, context).map_err(config_error)?;
    if !palace_exists(&config.palace_path) {
        return Ok(CliOutput::failure(
            1,
            format!(
                "{}\n\n## L1 — No palace found. Run: mempalace-cli init <dir> then mempalace-cli mine <dir>\n",
                default_identity_banner()
            ),
        ));
    }

    let runtime = build_runtime(&config).map_err(runtime_error)?;
    let engine = runtime
        .block_on(StorageEngine::open(&config.palace_path, config.embedding_profile))
        .map_err(storage_error)?;
    let provider = provider_factory(config.embedding_profile, default_embedding_cache_dir())
        .map_err(provider_error)?;
    let search = SearchRuntime::with_policy(
        provider,
        SearchRuntimePolicy { rerank_enabled: config.low_cpu.effective_rerank_enabled() },
    );
    let rendered = runtime
        .block_on(search.wake_up(
            engine.drawer_store(),
            &WakeUpRequest {
                wing: wing.as_deref().map(WingId::normalized).transpose().map_err(id_error)?,
                layer1: wake_up_layer1_config(&config),
                ..WakeUpRequest::default()
            },
        ))
        .map_err(search_error)?;

    let token_estimate = rendered.chars().count() / 4;
    Ok(CliOutput::success(format!(
        "Wake-up text (~{token_estimate} tokens):\n{}\n{}\n",
        "=".repeat(WAKE_UP_SEPARATOR_WIDTH),
        rendered
    )))
}

fn execute_serve<F, P>(
    bind_override: Option<SocketAddr>,
    token_file_override: Option<PathBuf>,
    palace_override: Option<&Path>,
    context: &CliContext,
    provider_factory: F,
) -> Result<CliOutput, clap::Error>
where
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>>,
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let config = load_runtime_config(palace_override, context).map_err(config_error)?;

    // Resolve bind address: CLI flag > config section > default (already in config)
    let bind = bind_override.unwrap_or(config.server.bind);

    // Resolve token file: CLI flag > config section > default
    let token_file = token_file_override.unwrap_or_else(|| config.server.token_file.clone());

    // Load the token registry — friendly error if the file is missing.
    let tokens = TokenRegistry::load(token_file.clone()).map_err(|err| {
        clap::Error::raw(
            clap::error::ErrorKind::Io,
            format!(
                "failed to load token file `{}`: {err}\n\n\
                 Hint: create the file with at least one entry, e.g.:\n\
                   [\n\
                     {{\"token\": \"<your-secret-token>\", \"name\": \"you\", \"enabled\": true}}\n\
                   ]\n",
                token_file.display()
            ),
        )
    })?;

    eprintln!(
        "WARNING: The federation server speaks plain HTTP. Bearer tokens must only be used \
         on trusted networks or behind a TLS-terminating reverse proxy."
    );
    eprintln!("Starting MemPalace federation server");
    eprintln!("  Palace:     {}", config.palace_path.display());
    eprintln!("  Bind:       {bind}");
    eprintln!("  Token file: {}", token_file.display());

    let runtime = build_runtime(&config).map_err(runtime_error)?;

    // Use MEMPALACE_STUB_EMBEDDINGS if set, mirroring the MCP binary.
    let serve_result = if std::env::var_os("MEMPALACE_STUB_EMBEDDINGS").is_some() {
        let provider = DeterministicStubProvider::new(config.embedding_profile);
        runtime.block_on(run_serve(config, provider, tokens, bind))
    } else {
        let provider = provider_factory(config.embedding_profile, default_embedding_cache_dir())
            .map_err(provider_error)?;
        runtime.block_on(run_serve(config, provider, tokens, bind))
    };

    match serve_result {
        Ok(()) => Ok(CliOutput::success("")),
        Err(err) => Ok(CliOutput::failure(1, format!("federation server error: {err}\n"))),
    }
}

async fn run_serve<P>(
    config: MempalaceConfig,
    provider: P,
    tokens: TokenRegistry,
    bind: SocketAddr,
) -> Result<(), mempalace_server::ServerError>
where
    P: EmbeddingProvider + Send + Sync + 'static,
{
    let router = build_router(config, provider, tokens).await?;
    let listener =
        tokio::net::TcpListener::bind(bind).await.map_err(|source| {
            mempalace_server::ServerError::Io {
                path: PathBuf::from(bind.to_string()),
                source,
            }
        })?;
    eprintln!(
        "Listening on http://{}",
        listener.local_addr().map_err(|source| mempalace_server::ServerError::Io {
            path: PathBuf::from("local_addr"),
            source,
        })?
    );
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            eprintln!("\nShutting down federation server...");
        })
        .await
        .map_err(|source| mempalace_server::ServerError::Io {
            path: PathBuf::from("axum::serve"),
            source,
        })
}

fn render_mine_summary(
    mode: CliMode,
    source_dir: &Path,
    palace_path: &Path,
    dry_run: bool,
    summary: &IngestSummary,
) -> String {
    let mode_name = match mode {
        CliMode::Projects => "projects",
        CliMode::Convos => "convos",
    };

    let mut lines = vec![
        format!("\n{}", "=".repeat(SEARCH_HEADER_WIDTH)),
        "  Mine complete".to_owned(),
        "=".repeat(SEARCH_HEADER_WIDTH),
        format!("  Mode: {mode_name}"),
        format!("  Source: {}", source_dir.display()),
        format!("  Palace: {}", palace_path.display()),
        format!("  Dry run: {}", if dry_run { "yes" } else { "no" }),
        format!("  Files discovered: {}", summary.discovered_files),
        format!("  Files ignored: {}", summary.ignored_files),
        format!("  Files unreadable: {}", summary.unreadable_files),
        format!("  Files malformed: {}", summary.malformed_files),
        format!("  Files skipped unchanged: {}", summary.skipped_unchanged),
        format!("  Files ingested: {}", summary.ingested_files),
        format!("  Drawers written: {}", summary.drawers_written),
        format!("  Files truncated: {}", summary.truncated_files),
    ];

    // Only print when non-zero to preserve byte-parity with existing test output.
    if summary.removed_sources > 0 {
        lines.push(format!("  Sources removed: {}", summary.removed_sources));
    }

    lines.push(format!("{}\n", "=".repeat(SEARCH_HEADER_WIDTH)));
    lines.join("\n")
}

fn deferred_command(command: &str) -> CliOutput {
    CliOutput::failure(
        1,
        format!(
            "The `{command}` command is deferred in Rust Phase 9.\nSee {DEFERRED_COMMAND_DOC} for the explicit scope decision.\n"
        ),
    )
}

fn no_palace_error(palace_path: &Path) -> CliOutput {
    CliOutput::failure(1, no_palace_message(palace_path))
}

fn no_palace_message(palace_path: &Path) -> String {
    format!(
        "\n  No palace found at {}\n  Run: mempalace-cli init <dir> then mempalace-cli mine <dir>\n",
        palace_path.display()
    )
}

fn palace_exists(palace_path: &Path) -> bool {
    let layout = StorageLayout::new(palace_path);
    palace_path.exists() && (layout.sqlite_path.exists() || layout.lancedb_dir.exists())
}

fn init_runtime_config(
    palace_override: Option<&Path>,
    context: &CliContext,
) -> Result<ResolvedPaths, mempalace_core::MempalaceError> {
    let paths = ConfigLoader::init_default(context.config_base_dir.as_deref())?;
    if let Some(palace_path) = palace_override {
        fs::create_dir_all(palace_path).map_err(|source| {
            mempalace_core::MempalaceError::ConfigWrite { path: palace_path.to_path_buf(), source }
        })?;
        write_global_config_override(&paths, palace_path)?;
    }
    Ok(paths)
}

fn load_runtime_config(
    palace_override: Option<&Path>,
    context: &CliContext,
) -> Result<MempalaceConfig, mempalace_core::MempalaceError> {
    let mut config = ConfigLoader::load_with_env(context.config_base_dir.as_deref())?;
    if let Some(palace_path) = palace_override {
        config.palace_path = palace_path.to_path_buf();
    }
    Ok(config)
}

fn write_global_config_override(
    paths: &ResolvedPaths,
    palace_path: &Path,
) -> Result<(), mempalace_core::MempalaceError> {
    let mut file = read_global_config_file(&paths.config_file)?;
    file.palace_path = Some(palace_path.display().to_string());
    let body = serde_json::to_string_pretty(&file).map_err(|source| {
        mempalace_core::MempalaceError::ConfigParse {
            path: paths.config_file.clone(),
            message: source.to_string(),
        }
    })?;
    fs::write(&paths.config_file, body).map_err(|source| {
        mempalace_core::MempalaceError::ConfigWrite { path: paths.config_file.clone(), source }
    })
}

fn read_global_config_file(
    config_path: &Path,
) -> Result<ConfigFileV1, mempalace_core::MempalaceError> {
    if !config_path.exists() {
        return Ok(ConfigFileV1::default());
    }

    let body = fs::read_to_string(config_path).map_err(|source| {
        mempalace_core::MempalaceError::ConfigRead { path: config_path.to_path_buf(), source }
    })?;
    serde_json::from_str(&body).map_err(|source| mempalace_core::MempalaceError::ConfigParse {
        path: config_path.to_path_buf(),
        message: source.to_string(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RoomDetection {
    source: &'static str,
    rooms: Vec<ProjectRoomConfig>,
}

fn detect_rooms(project_dir: &Path) -> std::io::Result<RoomDetection> {
    let mut discovered = BTreeMap::<String, BTreeSet<String>>::new();

    for entry in fs::read_dir(project_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();
        if SKIP_DIRS.contains(&name.as_str()) {
            continue;
        }
        record_room(&mut discovered, &name);
        for subentry in fs::read_dir(entry.path())? {
            let subentry = subentry?;
            if !subentry.file_type()?.is_dir() {
                continue;
            }

            let subname = subentry.file_name().to_string_lossy().to_string();
            if SKIP_DIRS.contains(&subname.as_str()) {
                continue;
            }
            record_room(&mut discovered, &subname);
        }
    }

    let (source, rooms) = if discovered.is_empty() {
        ("fallback", vec![project_room("general", "All project files", &["general"])])
    } else {
        (
            "folder structure",
            discovered
                .into_iter()
                .map(|(room, originals)| {
                    let original_dirs = originals.into_iter().collect::<Vec<_>>();
                    let description = if original_dirs.len() == 1 {
                        format!("Files from {}/", original_dirs[0])
                    } else {
                        format!("Files from {}/", original_dirs.join(", "))
                    };
                    let mut keywords = vec![room.clone()];
                    keywords.extend(original_dirs.iter().cloned());
                    project_room(
                        &room,
                        &description,
                        &keywords.iter().map(String::as_str).collect::<Vec<_>>(),
                    )
                })
                .collect::<Vec<_>>(),
        )
    };

    let mut deduped = BTreeMap::<String, ProjectRoomConfig>::new();
    for room in rooms {
        deduped.entry(room.name.clone()).or_insert(room);
    }
    let mut deduped = deduped.into_values().collect::<Vec<_>>();
    if !deduped.iter().any(|room| room.name == "general") {
        deduped.push(project_room("general", "Files that don't fit other rooms", &[]));
    }

    deduped.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(RoomDetection { source, rooms: deduped })
}

fn record_room(discovered: &mut BTreeMap<String, BTreeSet<String>>, raw_name: &str) {
    let normalized = raw_name.to_lowercase().replace('-', "_").replace(' ', "_");
    let room_name = FOLDER_ROOM_MAP
        .iter()
        .find_map(|(key, room)| (*key == normalized).then_some((*room).to_owned()))
        .unwrap_or_else(|| normalized);
    discovered.entry(room_name).or_default().insert(raw_name.to_owned());
}

fn project_room(name: &str, description: &str, keywords: &[&str]) -> ProjectRoomConfig {
    ProjectRoomConfig {
        name: name.to_owned(),
        description: Some(description.to_owned()),
        keywords: keywords.iter().map(|value| (*value).to_owned()).collect(),
    }
}

fn write_project_config(
    config_path: &Path,
    project_dir: &Path,
    project_config: &ProjectConfig,
    overwrite: bool,
) -> std::io::Result<()> {
    if config_path.exists() && !overwrite {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "{} already exists; re-run `mempalace-cli init {}` with `--yes` to overwrite it",
                config_path.display(),
                project_dir.display()
            ),
        ));
    }

    let mut root = Mapping::new();
    root.insert(
        serde_yaml::Value::String("wing".to_owned()),
        serde_yaml::Value::String(project_config.wing.clone()),
    );
    root.insert(
        serde_yaml::Value::String("rooms".to_owned()),
        serde_yaml::to_value(&project_config.rooms)
            .map_err(|error| std::io::Error::other(error.to_string()))?,
    );
    if let Some(routing) = &project_config.routing {
        root.insert(
            serde_yaml::Value::String("routing".to_owned()),
            serde_yaml::to_value(routing)
                .map_err(|error| std::io::Error::other(error.to_string()))?,
        );
    }

    fs::write(
        config_path,
        serde_yaml::to_string(&root).map_err(|error| std::io::Error::other(error.to_string()))?,
    )
}

fn wing_name_for_dir(project_dir: &Path) -> String {
    let name = project_dir
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("project")
        .to_lowercase()
        .replace('-', "_")
        .replace(' ', "_");
    if name.starts_with("wing_") { name } else { format!("wing_{name}") }
}

fn wing_ids_equal(left: &str, right: &str) -> bool {
    match (WingId::normalized(left), WingId::normalized(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left.eq_ignore_ascii_case(right),
    }
}

fn count_project_files(project_dir: &Path) -> std::io::Result<usize> {
    let mut total = 0usize;
    let mut stack = vec![project_dir.to_path_buf()];

    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            let name = entry.file_name().to_string_lossy().to_string();

            if file_type.is_dir() {
                if !SKIP_DIRS.contains(&name.as_str()) {
                    stack.push(path);
                }
                continue;
            }

            let extension = path.extension().and_then(|value| value.to_str()).unwrap_or_default();
            if PROJECT_FILE_EXTENSIONS.contains(&extension) {
                total += 1;
            }
        }
    }

    Ok(total)
}

fn default_embedding_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from(".cache"))
        .join("mempalace")
        .join("embeddings")
}

fn clamp_search_results(results: usize, config: &MempalaceConfig) -> usize {
    results.min(config.low_cpu.effective_search_results_limit())
}

fn wake_up_layer1_config(config: &MempalaceConfig) -> Layer1Config {
    let mut layer1 = Layer1Config::default();
    layer1.max_drawers = layer1.max_drawers.min(config.low_cpu.effective_wake_up_drawers_limit());
    layer1
}

fn default_identity_banner() -> &'static str {
    "## L0 — IDENTITY\nNo identity configured. Create ~/.mempalace/identity.txt"
}

fn fastembed_provider(
    profile: EmbeddingProfile,
    cache_root: PathBuf,
) -> Result<FastembedProvider, Box<dyn std::error::Error>> {
    Ok(FastembedProvider::new(profile, fastembed_provider_config(cache_root)).try_initialize()?)
}

fn fastembed_validation_provider(
    profile: EmbeddingProfile,
    cache_root: PathBuf,
) -> Result<FastembedProvider, Box<dyn std::error::Error>> {
    Ok(FastembedProvider::new(profile, fastembed_provider_config(cache_root)))
}

fn fastembed_provider_config(cache_root: PathBuf) -> FastembedProviderConfig {
    let mut config = FastembedProviderConfig::new(cache_root);
    if env_flag("MEMPALACE_EMBED_ALLOW_DOWNLOADS") {
        config.allow_downloads = true;
        config.show_download_progress = true;
    }
    config
}

fn config_error(error: mempalace_core::MempalaceError) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::Io, error.to_string())
}

fn ingest_error(error: mempalace_ingest::IngestError) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::Io, error.to_string())
}

fn provider_error<E>(error: E) -> clap::Error
where
    E: std::fmt::Display,
{
    clap::Error::raw(clap::error::ErrorKind::Io, error.to_string())
}

fn runtime_error(error: std::io::Error) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::Io, error.to_string())
}

fn search_error(error: mempalace_search::SearchError) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::Io, error.to_string())
}

fn storage_error(error: mempalace_storage::StorageError) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::Io, error.to_string())
}

fn io_error(error: std::io::Error) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::Io, error.to_string())
}

fn id_error(error: mempalace_core::IdError) -> clap::Error {
    clap::Error::raw(clap::error::ErrorKind::InvalidValue, error.to_string())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    use mempalace_core::EmbeddingProfileMetadata;
    use mempalace_embeddings::{
        EmbeddingRequest, EmbeddingResponse, StartupValidation, StartupValidationStatus,
    };
    use mempalace_storage::StorageLayout;
    use tempfile::tempdir;

    #[derive(Debug, Clone)]
    struct StubProvider {
        profile: EmbeddingProfile,
    }

    impl StubProvider {
        fn new(profile: EmbeddingProfile) -> Self {
            Self { profile }
        }
    }

    impl EmbeddingProvider for StubProvider {
        fn profile(&self) -> &'static EmbeddingProfileMetadata {
            self.profile.metadata()
        }

        fn startup_validation(&self) -> mempalace_embeddings::Result<StartupValidation> {
            Ok(StartupValidation {
                status: StartupValidationStatus::Ready,
                cache_root: PathBuf::from("/tmp/stub-cache"),
                model_id: self.profile.metadata().model_id,
                detail: "stub".to_owned(),
            })
        }

        fn embed(
            &mut self,
            request: &EmbeddingRequest,
        ) -> mempalace_embeddings::Result<EmbeddingResponse> {
            let dimensions = self.profile.metadata().dimensions;
            let vectors = request
                .texts()
                .iter()
                .map(|text| stub_vector(text, dimensions))
                .collect::<Vec<_>>();
            EmbeddingResponse::from_vectors(
                vectors,
                dimensions,
                self.profile,
                self.profile.metadata().model_id,
            )
        }
    }

    fn stub_provider(
        profile: EmbeddingProfile,
        _cache_root: PathBuf,
    ) -> Result<StubProvider, Box<dyn std::error::Error>> {
        Ok(StubProvider::new(profile))
    }

    fn stub_vector(text: &str, dimensions: usize) -> Vec<f32> {
        let lowered = text.to_lowercase();
        let seed = if lowered.contains("auth") || lowered.contains("login") {
            [1.0, 0.0, 0.0, 0.0]
        } else if lowered.contains("roadmap") || lowered.contains("plan") {
            [0.0, 1.0, 0.0, 0.0]
        } else {
            [0.0, 0.0, 1.0, 0.0]
        };

        let mut values = Vec::with_capacity(dimensions);
        while values.len() < dimensions {
            values.extend(seed);
        }
        values.truncate(dimensions);
        values
    }

    fn temp_config_root(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).expect("system clock").as_nanos();
        std::env::temp_dir().join(format!("mempalace-cli-{prefix}-{nanos}"))
    }

    fn remove_dir_all_if_exists(path: &Path) {
        if path.exists() {
            fs::remove_dir_all(path).unwrap();
        }
    }

    fn write_file(path: &Path, body: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create parent directories");
        }
        fs::write(path, body).expect("write fixture file");
    }

    fn setup_project_fixture(root: &Path) -> PathBuf {
        let project = root.join("project-alpha");
        write_file(
            &project.join("backend/auth.rs"),
            "Auth login flow keeps auth checks in the backend service.\n",
        );
        write_file(
            &project.join("docs/roadmap.md"),
            "Roadmap plan tracks the migration milestones and release plan.\n",
        );
        write_file(&project.join("README.md"), "Project overview for auth migration.\n");
        project
    }

    fn setup_second_project_fixture(root: &Path) -> PathBuf {
        let project = root.join("project-beta");
        write_file(
            &project.join("planning/roadmap.md"),
            "Roadmap ownership stays with the beta planning group.\n",
        );
        write_file(
            &project.join("backend/payments.rs"),
            "Payments ledger reconciliation runs in the beta backend.\n",
        );
        project
    }

    fn setup_convo_fixture(root: &Path) -> PathBuf {
        let convos = root.join("convos");
        write_file(
            &convos.join("session.txt"),
            "> What changed?\nWe fixed the auth migration.\n\n> Why?\nTo keep search results stable.\n",
        );
        convos
    }

    #[test]
    fn help_lists_phase9_commands_and_deferred_entries() {
        let output = run_cli(["--help"], &CliContext::production(), stub_provider).unwrap();
        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("init"));
        assert!(output.stdout.contains("mine"));
        assert!(output.stdout.contains("search"));
        assert!(output.stdout.contains("status"));
        assert!(output.stdout.contains("wake-up"));
        assert!(output.stdout.contains("split"));
        assert!(output.stdout.contains("compress"));
    }

    #[test]
    fn no_command_prints_help_and_exits_zero() {
        let output =
            run_cli(std::iter::empty::<&str>(), &CliContext::production(), stub_provider).unwrap();
        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("Usage:"));
    }

    #[test]
    fn deferred_commands_fail_with_explicit_record() {
        let split =
            run_cli(["split", "fixtures"], &CliContext::production(), stub_provider).unwrap();
        assert_eq!(split.exit_code, 1);
        assert!(split.stderr.contains(DEFERRED_COMMAND_DOC));

        let compress = run_cli(["compress"], &CliContext::production(), stub_provider).unwrap();
        assert_eq!(compress.exit_code, 1);
        assert!(compress.stderr.contains("deferred"));
    }

    #[test]
    fn init_registers_project_without_writing_repository_config_by_default() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("init");
        let context = CliContext::for_tests(config_root.clone());

        let output =
            run_cli(["init", project_dir.to_str().unwrap()], &context, stub_provider).unwrap();

        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("MemPalace Init"));
        assert!(!project_dir.join("mempalace.yaml").exists());
        assert!(config_root.join("projects.json").exists());
        assert!(config_root.join("config.json").exists());
        assert!(config_root.join("palace").exists());
        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn init_does_not_reuse_wing_only_id_for_an_unrelated_checkout() {
        let workspace = tempdir().unwrap();
        let first = setup_project_fixture(&workspace.path().join("first"));
        let second = setup_project_fixture(&workspace.path().join("second"));
        let config_root = temp_config_root("wing-only-id");
        let context = CliContext::for_tests(config_root.clone());

        let first_init = run_cli(["init", first.to_str().unwrap()], &context, stub_provider).unwrap();
        assert_eq!(first_init.exit_code, 0);
        let second_init = run_cli(["init", second.to_str().unwrap()], &context, stub_provider).unwrap();
        assert_eq!(second_init.exit_code, 1);
        assert!(second_init.stderr.contains("pass --project-id"));

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn init_imports_existing_project_config_without_modifying_repository_file() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("init-yes");
        let context = CliContext::for_tests(config_root.clone());
        let existing = "wing: preserved\nrooms:\n  - name: archive\n    description: Keep me\n";
        write_file(&project_dir.join("mempalace.yaml"), existing);

        let output =
            run_cli(["init", project_dir.to_str().unwrap()], &context, stub_provider).unwrap();
        assert_eq!(output.exit_code, 0);
        assert_eq!(fs::read_to_string(project_dir.join("mempalace.yaml")).unwrap(), existing);
        assert!(config_root.join("projects.json").is_file());

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn init_repo_config_flag_emits_portable_override() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("init-repo-config");
        let context = CliContext::for_tests(config_root.clone());

        let output = run_cli(
            ["init", project_dir.to_str().unwrap(), "--repo-config"],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_eq!(output.exit_code, 0);
        assert!(project_dir.join("mempalace.yaml").is_file());
        assert!(config_root.join("projects.json").is_file());
        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn init_with_palace_override_preserves_existing_global_config_fields() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("init-palace");
        fs::create_dir_all(&config_root).unwrap();
        let context = CliContext::for_tests(config_root.clone());
        write_file(
            &config_root.join("config.json"),
            r#"{
  "version": 1,
  "palace_path": "/tmp/original-palace",
  "collection_name": "custom_collection",
  "embedding_profile": "low_cpu"
}"#,
        );
        let override_palace = workspace.path().join("custom-palace");

        let output = run_cli(
            [
                "--palace",
                override_palace.to_str().unwrap(),
                "init",
                project_dir.to_str().unwrap(),
                "--yes",
            ],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_eq!(output.exit_code, 0);
        let config: ConfigFileV1 =
            serde_json::from_str(&fs::read_to_string(config_root.join("config.json")).unwrap())
                .unwrap();
        assert_eq!(config.collection_name, "custom_collection");
        assert_eq!(config.embedding_profile, Some(EmbeddingProfile::LowCpu));
        assert_eq!(config.palace_path, Some(override_palace.display().to_string()));
        assert_eq!(config.low_cpu, None);

        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn low_cpu_search_results_are_clamped_by_runtime_config() {
        let workspace = tempdir().unwrap();
        let project_alpha = setup_project_fixture(workspace.path());
        let project_beta = setup_second_project_fixture(workspace.path());
        let balanced_root = temp_config_root("balanced-search");
        let balanced_context = CliContext::for_tests(balanced_root.clone());
        let config_root = temp_config_root("low-cpu-search");
        fs::create_dir_all(&balanced_root).unwrap();
        fs::create_dir_all(&config_root).unwrap();
        write_file(
            &config_root.join("config.json"),
            r#"{
  "version": 1,
  "collection_name": "mempalace_drawers",
  "embedding_profile": "low_cpu",
  "low_cpu": {
    "search_results_limit": 1
  }
}"#,
        );
        let context = CliContext::for_tests(config_root.clone());

        run_cli(
            ["init", project_alpha.to_str().unwrap(), "--yes"],
            &balanced_context,
            stub_provider,
        )
        .unwrap();
        run_cli(
            ["init", project_beta.to_str().unwrap(), "--yes"],
            &balanced_context,
            stub_provider,
        )
        .unwrap();
        run_cli(["mine", project_alpha.to_str().unwrap()], &balanced_context, stub_provider)
            .unwrap();
        run_cli(["mine", project_beta.to_str().unwrap()], &balanced_context, stub_provider)
            .unwrap();
        run_cli(["init", project_alpha.to_str().unwrap(), "--yes"], &context, stub_provider)
            .unwrap();
        run_cli(["init", project_beta.to_str().unwrap(), "--yes"], &context, stub_provider)
            .unwrap();
        run_cli(["mine", project_alpha.to_str().unwrap()], &context, stub_provider).unwrap();
        run_cli(["mine", project_beta.to_str().unwrap()], &context, stub_provider).unwrap();

        let unclamped =
            run_cli(["search", "roadmap", "--results", "5"], &balanced_context, stub_provider)
                .unwrap();
        let search =
            run_cli(["search", "roadmap", "--results", "5"], &context, stub_provider).unwrap();

        assert_eq!(unclamped.exit_code, 0);
        assert!(unclamped.stdout.matches("  [").count() > 1);
        assert_eq!(search.exit_code, 0);
        assert_eq!(search.stdout.matches("  [").count(), 1);

        fs::remove_dir_all(balanced_root).unwrap();
        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn low_cpu_wake_up_drawers_are_clamped_by_runtime_config() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("low-cpu-wake-up");
        fs::create_dir_all(&config_root).unwrap();
        write_file(
            &config_root.join("config.json"),
            r#"{
  "version": 1,
  "collection_name": "mempalace_drawers",
  "embedding_profile": "low_cpu",
  "low_cpu": {
    "degraded_mode": false,
    "wake_up_drawers_limit": 1
  }
}"#,
        );
        let context = CliContext::for_tests(config_root.clone());

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();
        run_cli(["mine", project_dir.to_str().unwrap()], &context, stub_provider).unwrap();

        let wake_up = run_cli(["wake-up"], &context, stub_provider).unwrap();

        assert_eq!(wake_up.exit_code, 0);
        assert_eq!(wake_up.stdout.matches("  - ").count(), 1);

        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn mine_search_status_and_wakeup_work_end_to_end() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("e2e");
        let context = CliContext::for_tests(config_root.clone());

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        let mine =
            run_cli(["mine", project_dir.to_str().unwrap()], &context, stub_provider).unwrap();
        assert_eq!(mine.exit_code, 0);
        assert!(mine.stdout.contains("Files ingested: 3"));

        let search = run_cli(["search", "auth login"], &context, stub_provider).unwrap();
        assert_eq!(search.exit_code, 0);
        assert!(search.stdout.contains("Results for: \"auth login\""));
        assert!(search.stdout.contains("backend"));

        let status = run_cli(["status"], &context, stub_provider).unwrap();
        assert_eq!(status.exit_code, 0);
        assert!(status.stdout.contains("MemPalace Status"));
        assert!(status.stdout.contains("WING: wing_project_alpha"));

        let wake_up = run_cli(["wake-up"], &context, stub_provider).unwrap();
        assert_eq!(wake_up.exit_code, 0);
        assert!(wake_up.stdout.contains("Wake-up text"));
        assert!(wake_up.stdout.contains("ESSENTIAL STORY"));

        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn mine_accepts_batch_size_on_local_path() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("batch-size-local");
        let context = CliContext::for_tests(config_root.clone());

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        // --batch-size 1 forces single-chunk embed batches; mining still ingests
        // every file, proving the flag is accepted and threads through cleanly.
        let mine = run_cli(
            ["mine", project_dir.to_str().unwrap(), "--batch-size", "1"],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(mine.exit_code, 0, "stderr: {}", mine.stderr);
        assert!(
            mine.stdout.contains("Files ingested: 3"),
            "expected 3 files ingested with --batch-size 1: {}",
            mine.stdout
        );

        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn build_remote_batches_respects_max_files() {
        use mempalace_federation::{IngestChunkDto, IngestFileDto};

        let make_file = |name: &str| IngestFileDto {
            relative_path: name.to_owned(),
            content_hash: "hash".to_owned(),
            file_hash: None,
            chunks: vec![IngestChunkDto {
                chunk_index: 0,
                room: "general".to_owned(),
                text: "x".to_owned(),
                byte_start: None,
                byte_end: None,
                line_start: None,
                line_end: None,
            }],
        };

        let files: Vec<_> = (0..5).map(|i| make_file(&format!("f{i}.rs"))).collect();

        // Cap of 2 files/batch over 5 tiny files → 3 batches sized [2, 2, 1].
        let batches = build_remote_batches(files.clone(), 2);
        assert_eq!(batches.len(), 3, "expected 3 batches at cap 2");
        assert!(batches.iter().all(|b| b.len() <= 2), "no batch may exceed the cap");
        assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), 5, "no file dropped");

        // 0 falls back to the default cap → a single batch for 5 tiny files.
        let batches = build_remote_batches(files, 0);
        assert_eq!(batches.len(), 1, "0 must fall back to the default cap");
        assert_eq!(batches[0].len(), 5);
    }

    #[test]
    fn render_remote_mine_summary_labels_partial_on_incomplete() {
        let src = Path::new("/tmp/src");

        // No per-file failures, but replication_incomplete=true → "partial — transport interrupted".
        let output = render_remote_mine_summary(
            src, "hub", "http://example.com", "wing_test", "repo-1",
            5, 0, 0, 5, &[], &[], None, true, true,
        );
        assert!(
            output.contains("replication: partial"),
            "incomplete=true must produce 'partial', got: {output}",
        );
        assert!(
            output.contains("transport interrupted"),
            "incomplete=true without file errors must say 'transport interrupted', got: {output}",
        );
        assert!(
            !output.contains("replication: succeeded"),
            "incomplete=true must NOT say 'succeeded', got: {output}",
        );

        // No per-file failures, replication_incomplete=false → "succeeded".
        let output = render_remote_mine_summary(
            src, "hub", "http://example.com", "wing_test", "repo-1",
            5, 0, 0, 5, &[], &[], None, true, false,
        );
        assert!(
            output.contains("replication: succeeded"),
            "incomplete=false + no file failures must say 'succeeded', got: {output}",
        );

        // Per-file failures, replication_incomplete=false → "partial — some files had errors".
        let output = render_remote_mine_summary(
            src, "hub", "http://example.com", "wing_test", "repo-1",
            3, 0, 2, 3, &[], &[("bad.rs".into(), "err".into())], None, true, false,
        );
        assert!(
            output.contains("replication: partial"),
            "file failures must produce 'partial', got: {output}",
        );
        assert!(
            output.contains("some files had errors"),
            "file failures must say 'some files had errors', got: {output}",
        );

        // Both incomplete AND per-file failures → "partial — transport interrupted".
        let output = render_remote_mine_summary(
            src, "hub", "http://example.com", "wing_test", "repo-1",
            3, 0, 2, 3, &[], &[("bad.rs".into(), "err".into())], None, true, true,
        );
        assert!(
            output.contains("replication: partial"),
            "incomplete=true with file failures must produce 'partial', got: {output}",
        );
        assert!(
            output.contains("transport interrupted"),
            "incomplete=true with file failures must say 'transport interrupted', got: {output}",
        );
        assert!(
            !output.contains("some files had errors"),
            "incomplete=true must not say 'some files had errors', got: {output}",
        );
        assert!(
            !output.contains("replication: succeeded"),
            "incomplete=true with file failures must NOT say 'succeeded', got: {output}",
        );

        // Non dual_write: no replication label at all.
        let output = render_remote_mine_summary(
            src, "hub", "http://example.com", "wing_test", "repo-1",
            5, 0, 0, 5, &[], &[], None, false, false,
        );
        assert!(
            !output.contains("replication:"),
            "non-dual-write must not contain 'replication:', got: {output}",
        );
    }

    #[test]
    fn dual_write_surfaces_remote_stderr_without_failing_local_success() {
        let output = combine_dual_write_outputs(
            CliOutput::success("local mine complete"),
            CliOutput::failure(1, "remote replication failed"),
        );

        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("local mine complete"));
        assert!(output.stdout.contains("remote replication failed"));
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn mine_dry_run_reports_work_without_writing_storage_files() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("dry-run");
        let context = CliContext::for_tests(config_root.clone());

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        let output = run_cli(
            ["mine", project_dir.to_str().unwrap(), "--dry-run", "--limit", "1"],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("Dry run: yes"));
        assert!(output.stdout.contains("Files ingested: 1"));

        let status = run_cli(["status"], &context, stub_provider).unwrap();
        assert_eq!(status.exit_code, 1);
        assert!(status.stderr.contains("No palace found"));
        let layout = StorageLayout::new(config_root.join("palace"));
        assert!(!layout.sqlite_path.exists());
        assert!(!layout.lancedb_dir.exists());
        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn mine_invalid_source_path_fails_before_creating_storage() {
        let workspace = tempdir().unwrap();
        let config_root = temp_config_root("invalid-source");
        let context = CliContext::for_tests(config_root.clone());

        let error = run_cli(
            ["mine", workspace.path().join("missing-dir").to_str().unwrap()],
            &context,
            stub_provider,
        )
        .unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::Io);
        assert!(error.to_string().contains("failed to access source directory"));
        let layout = StorageLayout::new(config_root.join("palace"));
        assert!(!layout.sqlite_path.exists());
        assert!(!layout.lancedb_dir.exists());

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn search_without_a_palace_exits_non_zero() {
        let config_root = temp_config_root("missing");
        fs::create_dir_all(&config_root).unwrap();
        let context = CliContext::for_tests(config_root.clone());

        let output = run_cli(["search", "auth"], &context, stub_provider).unwrap();
        assert_eq!(output.exit_code, 1);
        assert!(output.stderr.contains("No palace found"));
        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn status_without_a_palace_exits_non_zero() {
        let config_root = temp_config_root("missing-status");
        fs::create_dir_all(&config_root).unwrap();
        let context = CliContext::for_tests(config_root.clone());

        let output = run_cli(["status"], &context, stub_provider).unwrap();
        assert_eq!(output.exit_code, 1);
        assert!(output.stderr.contains("No palace found"));
        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn wake_up_without_a_palace_exits_non_zero() {
        let config_root = temp_config_root("missing-wake-up");
        fs::create_dir_all(&config_root).unwrap();
        let context = CliContext::for_tests(config_root.clone());

        let output = run_cli(["wake-up"], &context, stub_provider).unwrap();
        assert_eq!(output.exit_code, 1);
        assert!(output.stderr.contains("No palace found"));

        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn mine_projects_respects_wing_override() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("wing-override");
        let context = CliContext::for_tests(config_root.clone());

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();
        let output = run_cli(
            ["mine", project_dir.to_str().unwrap(), "--wing", "overridewing"],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(output.exit_code, 0);

        let status = run_cli(["status"], &context, stub_provider).unwrap();
        // --wing overridewing is normalized to wing_overridewing so that explicit
        // CLI overrides match the wing_-prefixed convention used everywhere else.
        assert!(status.stdout.contains("WING: wing_overridewing"));
        assert!(!status.stdout.contains("WING: wing_project_alpha"));
        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn mine_projects_uses_central_registry_without_repository_yaml() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("central-registry-mine");
        let context = CliContext::for_tests(config_root.clone());

        let init = run_cli(["init", project_dir.to_str().unwrap()], &context, stub_provider)
            .unwrap();
        assert_eq!(init.exit_code, 0);
        assert!(!project_dir.join("mempalace.yaml").exists());

        let mine = run_cli(
            ["mine", project_dir.to_str().unwrap(), "--dry-run"],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(mine.exit_code, 0);
        assert!(mine.stdout.contains("Dry run: yes"));

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn project_commands_manage_registry_and_export_compatibility_file() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("project-commands");
        let context = CliContext::for_tests(config_root.clone());
        let project_id = "local/project-alpha";

        let register = run_cli(
            [
                "project",
                "register",
                project_dir.to_str().unwrap(),
                "--wing",
                "wing_registered",
                "--project-id",
                project_id,
            ],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(register.exit_code, 0);
        assert!(register.stdout.contains(project_id));

        let list = run_cli(["project", "list"], &context, stub_provider).unwrap();
        assert!(list.stdout.contains("Projects: 1"));
        assert!(list.stdout.contains(project_id));

        let show = run_cli(
            ["project", "show", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        )
        .unwrap();
        assert!(show.stdout.contains("wing_registered"));

        let export = run_cli(
            ["project", "export", project_id, "--repo-config"],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(export.exit_code, 0);
        assert!(fs::read_to_string(project_dir.join("mempalace.yaml"))
            .unwrap()
            .contains("wing_registered"));

        let show_after_export = run_cli(
            ["project", "show", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        )
        .unwrap();
        assert!(show_after_export.stdout.contains("Project: local/project-alpha"));

        let remove = run_cli(["project", "remove", project_id], &context, stub_provider).unwrap();
        assert_eq!(remove.exit_code, 0);
        let list = run_cli(["project", "list"], &context, stub_provider).unwrap();
        assert!(list.stdout.contains("Projects: 0"));
        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn project_export_requires_explicit_repo_config_flag() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("project-export-flag");
        let context = CliContext::for_tests(config_root.clone());
        let project_id = "local/project-export";

        let register = run_cli(
            [
                "project",
                "register",
                project_dir.to_str().unwrap(),
                "--wing",
                "wing_export",
                "--project-id",
                project_id,
            ],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(register.exit_code, 0);

        let export = run_cli(["project", "export", project_id], &context, stub_provider).unwrap();
        assert_eq!(export.exit_code, 1);
        assert!(export.stderr.contains("requires --repo-config"));
        assert!(!project_dir.join("mempalace.yaml").exists());

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn equivalent_wing_ids_preserve_project_routing() {
        assert!(wing_ids_equal("wing_app", "app"));
        assert!(wing_ids_equal("APP", "wing_app"));
        assert!(!wing_ids_equal("wing_app", "wing_other"));
    }

    #[test]
    fn mine_convos_mode_supports_real_and_dry_run_paths() {
        let workspace = tempdir().unwrap();
        let convo_dir = setup_convo_fixture(workspace.path());
        let config_root = temp_config_root("convos");
        let context = CliContext::for_tests(config_root.clone());

        let mine = run_cli(
            [
                "--palace",
                workspace.path().join("convo-palace").to_str().unwrap(),
                "mine",
                convo_dir.to_str().unwrap(),
                "--mode",
                "convos",
                "--wing",
                "talks",
            ],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(mine.exit_code, 0);
        assert!(mine.stdout.contains("Mode: convos"));

        let dry_run = run_cli(
            [
                "--palace",
                workspace.path().join("dry-convo-palace").to_str().unwrap(),
                "mine",
                convo_dir.to_str().unwrap(),
                "--mode",
                "convos",
                "--wing",
                "talks",
                "--dry-run",
            ],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(dry_run.exit_code, 0);
        assert!(dry_run.stdout.contains("Dry run: yes"));

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn search_filters_and_wake_up_wing_flag_work_through_cli() {
        let workspace = tempdir().unwrap();
        let project_alpha = setup_project_fixture(workspace.path());
        let project_beta = setup_second_project_fixture(workspace.path());
        let config_root = temp_config_root("filters");
        let context = CliContext::for_tests(config_root.clone());

        run_cli(["init", project_alpha.to_str().unwrap(), "--yes"], &context, stub_provider)
            .unwrap();
        run_cli(["mine", project_alpha.to_str().unwrap()], &context, stub_provider).unwrap();
        run_cli(["init", project_beta.to_str().unwrap(), "--yes"], &context, stub_provider)
            .unwrap();
        run_cli(["mine", project_beta.to_str().unwrap()], &context, stub_provider).unwrap();

        let wing_search = run_cli(
            ["search", "reconciliation", "--wing", "wing_project_beta"],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(wing_search.exit_code, 0);
        assert!(wing_search.stdout.contains("wing_project_beta"));
        assert!(!wing_search.stdout.contains("wing_project_alpha"));

        let room_search =
            run_cli(["search", "roadmap", "--room", "planning"], &context, stub_provider).unwrap();
        assert_eq!(room_search.exit_code, 0);
        assert!(room_search.stdout.contains("planning"));

        let wake_up =
            run_cli(["wake-up", "--wing", "wing_project_beta"], &context, stub_provider).unwrap();
        assert_eq!(wake_up.exit_code, 0);
        assert!(wake_up.stdout.contains("[planning]"));
        assert!(wake_up.stdout.contains("roadmap.md"));
        assert!(!wake_up.stdout.contains("auth.rs"));

        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn detect_rooms_keeps_all_directory_aliases_for_same_room() {
        let workspace = tempdir().unwrap();
        fs::create_dir_all(workspace.path().join("frontend")).unwrap();
        fs::create_dir_all(workspace.path().join("client")).unwrap();

        let detection = detect_rooms(workspace.path()).unwrap();
        let frontend = detection.rooms.iter().find(|room| room.name == "frontend").unwrap();
        assert!(frontend.description.as_ref().unwrap().contains("frontend"));
        assert!(frontend.description.as_ref().unwrap().contains("client"));
        assert!(frontend.keywords.contains(&"frontend".to_owned()));
        assert!(frontend.keywords.contains(&"client".to_owned()));
    }

    #[test]
    fn fastembed_provider_initializes_or_fails_during_factory_creation() {
        let cache_root = tempdir().unwrap();
        let result =
            fastembed_provider(EmbeddingProfile::Balanced, cache_root.path().to_path_buf());
        assert!(result.is_err());
    }

    // ─── --branch tests ──────────────────────────────────────────────────────

    /// Initialize a bare git repo at `dir` with an initial commit on `main`.
    fn git_init_repo(dir: &Path, files: &[(&str, &str)]) {
        let run = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .args(args)
                .current_dir(dir)
                .status()
                .unwrap();
            assert!(status.success(), "git {args:?} failed in {}", dir.display());
        };
        run(&["init", "-b", "main"]);
        run(&["config", "user.email", "test@test.com"]);
        run(&["config", "user.name", "Test"]);
        for (path, content) in files {
            let full = dir.join(path);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&full, content).unwrap();
            // Force-add so a developer's global gitignore (e.g. one that excludes
            // `mempalace.yaml`) can't silently block staging the fixture file.
            run(&["add", "-f", path]);
        }
        run(&[
            "-c", "user.email=test@test.com",
            "-c", "user.name=Test",
            "commit", "-m", "initial",
        ]);
    }

    #[test]
    fn mine_branch_e2e_ingests_delta_file() {
        let workspace = tempdir().unwrap();
        let repo_dir = workspace.path().join("repo");
        fs::create_dir_all(&repo_dir).unwrap();

        let base_content = "fn base() -> i32 { 42 }\n".repeat(20);
        git_init_repo(
            &repo_dir,
            &[
                ("mempalace.yaml", "wing: branchtest\nrooms:\n  - name: general\n"),
                ("base.rs", &base_content),
                ("stable.rs", "fn stable() -> &str { \"hello\" }\n"),
            ],
        );

        // Create feature branch, modify one file.
        let run_git = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&repo_dir)
                .status()
                .unwrap();
        };
        run_git(&["checkout", "-b", "feature"]);
        let changed = "fn base() -> i32 { 99 }\n".repeat(20);
        fs::write(repo_dir.join("base.rs"), &changed).unwrap();

        let config_root = temp_config_root("mine-branch-e2e");
        let context = CliContext::for_tests(config_root.clone());

        let output = run_cli(
            ["mine", repo_dir.to_str().unwrap(), "--branch"],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_eq!(output.exit_code, 0, "branch mine failed: {:?}", output.stderr);
        // The modified file (base.rs) must be counted as ingested.
        assert!(
            output.stdout.contains("Files ingested: 1"),
            "expected 1 ingested file: {}",
            output.stdout
        );

        // Second run after reverting to original should show Sources removed: 1.
        fs::write(repo_dir.join("base.rs"), &base_content).unwrap();
        let second = run_cli(
            ["mine", repo_dir.to_str().unwrap(), "--branch"],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(second.exit_code, 0, "second branch mine failed: {:?}", second.stderr);
        assert!(
            second.stdout.contains("Sources removed: 1"),
            "expected 'Sources removed: 1' after revert: {}",
            second.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_branch_outside_git_repo_fails_cleanly() {
        let workspace = tempdir().unwrap();
        // Non-git directory (no .git).
        let project_dir = workspace.path().join("plain_project");
        fs::create_dir_all(&project_dir).unwrap();
        write_file(
            &project_dir.join("mempalace.yaml"),
            "wing: testproject\nrooms:\n  - name: general\n",
        );
        write_file(&project_dir.join("code.rs"), &"fn x() {}\n".repeat(20));

        let config_root = temp_config_root("branch-no-git");
        let context = CliContext::for_tests(config_root.clone());

        let result = run_cli(
            ["mine", project_dir.to_str().unwrap(), "--branch"],
            &context,
            stub_provider,
        );

        // Either a clap Err or an Ok with non-zero exit code — both count as failure.
        match result {
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("git") || msg.contains("branch"),
                    "error message should mention git: {msg}"
                );
            }
            Ok(output) => {
                assert_ne!(output.exit_code, 0, "should fail outside git repo");
                assert!(
                    output.stderr.contains("git") || output.stderr.contains("branch"),
                    "error message should mention git: {}",
                    output.stderr
                );
            }
        }

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_branch_with_convos_mode_fails() {
        let workspace = tempdir().unwrap();
        let convo_dir = setup_convo_fixture(workspace.path());
        let config_root = temp_config_root("branch-convos");
        let context = CliContext::for_tests(config_root.clone());

        let output = run_cli(
            [
                "mine",
                convo_dir.to_str().unwrap(),
                "--mode",
                "convos",
                "--wing",
                "talks",
                "--branch",
            ],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_ne!(output.exit_code, 0, "--branch + --mode convos should fail");
        assert!(
            output.stderr.contains("convos") || output.stderr.contains("projects"),
            "error should mention mode restriction: {}",
            output.stderr
        );

        remove_dir_all_if_exists(&config_root);
    }

    // ─── Remote mine e2e tests ───────────────────────────────────────────────

    fn restrict_token_file(path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = fs::metadata(path).unwrap().permissions();
            permissions.set_mode(0o600);
            fs::set_permissions(path, permissions).unwrap();
        }

        #[cfg(not(unix))]
        {
            let _ = path;
        }
    }

    /// Write a minimal token file into `dir` and return the path.
    fn write_test_token_file(dir: &Path, token: &str) -> PathBuf {
        let path = dir.join("tokens.json");
        fs::write(
            &path,
            serde_json::to_string(&serde_json::json!([
                {"token": token, "name": "test-user", "enabled": true}
            ]))
            .unwrap(),
        )
        .unwrap();
        restrict_token_file(&path);
        path
    }

    /// Build a `MempalaceConfig` pointing at the given palace dir with stub embeddings.
    fn remote_test_config(palace_dir: PathBuf, token_file: PathBuf) -> mempalace_config::MempalaceConfig {
        use mempalace_config::{
            FederationRuntimeConfig, LowCpuRuntimeConfig, MaintenanceRuntimeConfig,
            ServerRuntimeConfig,
        };
        mempalace_config::MempalaceConfig {
            schema_version: 1,
            collection_name: "mempalace_drawers".to_owned(),
            palace_path: palace_dir,
            embedding_profile: EmbeddingProfile::Balanced,
            low_cpu: LowCpuRuntimeConfig::defaults_for_profile(EmbeddingProfile::Balanced),
            server: ServerRuntimeConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                token_file,
                checkouts: std::collections::BTreeMap::new(),
            },
            federation: FederationRuntimeConfig::default(),
            maintenance: MaintenanceRuntimeConfig::defaults(),
        }
    }

    /// Spawn a federation server in-process on an ephemeral port.
    /// Returns the bound address.
    fn spawn_test_server(palace_dir: PathBuf, token: &str) -> std::net::SocketAddr {
        use mempalace_embeddings::DeterministicStubProvider;
        use mempalace_server::{TokenRegistry, build_router};

        let token_dir = tempfile::tempdir().unwrap();
        let token_file = write_test_token_file(token_dir.path(), token);
        let config = remote_test_config(palace_dir, token_file.clone());

        // Build a dedicated tokio runtime to host the server.
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let tokens = rt.block_on(async { TokenRegistry::load(token_file).unwrap() });
        let provider = DeterministicStubProvider::new(EmbeddingProfile::Balanced);
        let router = rt.block_on(build_router(config, provider, tokens)).unwrap();

        let listener =
            rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap();
        let addr = rt.block_on(async { listener.local_addr().unwrap() });

        rt.spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        // Keep the runtime alive by leaking it — the test process owns it for its lifetime.
        std::mem::forget(rt);
        // Keep token_dir alive too.
        std::mem::forget(token_dir);

        addr
    }

    /// Spawn a minimal federation server whose `/v1/info` response does NOT
    /// advertise the `ingest` capability. Used to test the unsupported-remote
    /// guard in the dual-write path.
    fn spawn_no_ingest_server(token: &str) -> std::net::SocketAddr {
        use axum::{
            Router, middleware,
            extract::State,
            http::{StatusCode, header},
            response::IntoResponse,
            routing::get,
        };
        use std::sync::Arc;

        let token_dir = tempfile::tempdir().unwrap();
        let token_file = write_test_token_file(token_dir.path(), token);
        let tokens = mempalace_server::TokenRegistry::load(token_file).unwrap();
        let tokens = Arc::new(tokens);

        #[derive(Clone)]
        struct NoIngestState {
            tokens: Arc<mempalace_server::TokenRegistry>,
        }

        async fn auth(
            State(state): State<NoIngestState>,
            request: axum::http::Request<axum::body::Body>,
            next: middleware::Next,
        ) -> impl IntoResponse {
            let token = request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.strip_prefix("Bearer "));
            match token.and_then(|t| state.tokens.authenticate(t)) {
                Some(_) => next.run(request).await.into_response(),
                None => StatusCode::UNAUTHORIZED.into_response(),
            }
        }

        async fn info() -> impl axum::response::IntoResponse {
            axum::Json(mempalace_federation::InfoResponse {
                server_version: env!("CARGO_PKG_VERSION").to_owned(),
                federation_api_version: mempalace_federation::FEDERATION_API_VERSION,
                embedding_profile: "balanced".to_owned(),
                capabilities: vec![
                    "drawers".to_owned(),
                    "kg".to_owned(),
                    "changes".to_owned(),
                    "taxonomy".to_owned(),
                ],
            })
        }

        let state = NoIngestState { tokens };
        let router = Router::new()
            .route("/v1/info", get(info))
            .layer(middleware::from_fn_with_state(state.clone(), auth))
            .with_state(state);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let listener =
            rt.block_on(tokio::net::TcpListener::bind("127.0.0.1:0")).unwrap();
        let addr = rt.block_on(async { listener.local_addr().unwrap() });
        rt.spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        std::mem::forget(rt);
        std::mem::forget(token_dir);

        addr
    }

    /// Write a CLI config.json with federation remotes pointing at `server_url`,
    /// with an inline `token`. Also writes a per-project mempalace.yaml.
    fn write_remote_cli_config(
        config_dir: &Path,
        remote_name: &str,
        server_url: &str,
        inline_token: &str,
        wing_name: &str,
        project_dir: &Path,
    ) {
        fs::create_dir_all(config_dir).unwrap();
        // Use inline token (not token_env) to avoid unsafe set_var in Rust 2024.
        let config_json = serde_json::json!({
            "version": 1,
            "federation": {
                "remotes": [{"name": remote_name, "url": server_url, "token": inline_token}],
                "wings": {
                    wing_name: {"mode": "remote", "remote": remote_name}
                }
            }
        });
        fs::write(
            config_dir.join("config.json"),
            serde_json::to_string_pretty(&config_json).unwrap(),
        )
        .unwrap();

        // Write the project config.
        let yaml = format!(
            "wing: {wing_name}\nrooms:\n  - name: general\n    description: General files\n"
        );
        fs::write(project_dir.join("mempalace.yaml"), yaml).unwrap();
    }

    /// Write a CLI config for `combined` + `write: both` routing.
    fn write_combined_cli_config(
        config_dir: &Path,
        remote_name: &str,
        server_url: &str,
        inline_token: &str,
        wing_name: &str,
        palace_dir: &Path,
        project_dir: &Path,
    ) {
        fs::create_dir_all(config_dir).unwrap();
        let config_json = serde_json::json!({
            "version": 1,
            "palace_path": palace_dir.to_str().unwrap(),
            "federation": {
                "remotes": [{"name": remote_name, "url": server_url, "token": inline_token}],
                "wings": {
                    wing_name: {"mode": "combined", "write": "both", "remote": remote_name}
                }
            }
        });
        fs::write(
            config_dir.join("config.json"),
            serde_json::to_string_pretty(&config_json).unwrap(),
        )
        .unwrap();

        let yaml = format!(
            "wing: {wing_name}\nrooms:\n  - name: general\n    description: General files\n"
        );
        fs::write(project_dir.join("mempalace.yaml"), yaml).unwrap();
    }

    #[test]
    fn mine_remote_routed_e2e_ingests_to_server() {
        const TOKEN: &str = "remote-mine-e2e-tok-001";
        let workspace = tempdir().unwrap();

        // Server palace dir.
        let server_palace = workspace.path().join("server-palace");
        fs::create_dir_all(&server_palace).unwrap();

        let addr = spawn_test_server(server_palace.clone(), TOKEN);
        let server_url = format!("http://{addr}");

        // Project dir.
        let project_dir = workspace.path().join("myproject");
        fs::create_dir_all(&project_dir).unwrap();
        write_file(
            &project_dir.join("backend/auth.rs"),
            "Auth login flow keeps auth checks in the backend service.\n"
                .repeat(5)
                .as_str(),
        );
        write_file(
            &project_dir.join("docs/roadmap.md"),
            "Roadmap plan tracks the migration milestones.\n".repeat(5).as_str(),
        );

        // CLI config dir.
        let config_root = temp_config_root("remote-mine-e2e");
        let wing_name = "wing_myproject";
        let remote_name = "hub";
        write_remote_cli_config(
            &config_root,
            remote_name,
            &server_url,
            TOKEN,
            wing_name,
            &project_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let output = run_cli(
            ["mine", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_eq!(output.exit_code, 0, "remote mine failed: stderr={:?}", output.stderr);
        assert!(
            output.stdout.contains("Remote:"),
            "summary must show 'Remote:': {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("Files ingested:"),
            "summary must show ingested count: {}",
            output.stdout
        );
        // Files ingested must be > 0.
        let ingested_line =
            output.stdout.lines().find(|l| l.contains("Files ingested:")).unwrap_or("");
        let ingested_n: usize =
            ingested_line.split(':').last().unwrap_or("0").trim().parse().unwrap_or(0);
        assert!(ingested_n > 0, "should have ingested at least 1 file: {}", output.stdout);

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_remote_dry_run_prints_plan_without_server() {
        // No server running; dry-run should succeed without network.
        let workspace = tempdir().unwrap();
        let project_dir = workspace.path().join("dryrunproject");
        fs::create_dir_all(&project_dir).unwrap();
        write_file(
            &project_dir.join("code.rs"),
            "fn example() -> bool { true }\n".repeat(10).as_str(),
        );

        let config_root = temp_config_root("remote-dry-run");
        let wing_name = "wing_dryrunproject";
        let remote_name = "hub";
        // Port 1 is not bindable, so any real connection would fail — but we're dry-run.
        let server_url = "http://127.0.0.1:1";
        write_remote_cli_config(
            &config_root,
            remote_name,
            server_url,
            "dryrun-tok",
            wing_name,
            &project_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let output = run_cli(
            ["mine", project_dir.to_str().unwrap(), "--dry-run"],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_eq!(output.exit_code, 0, "dry-run should succeed without server: {:?}", output);
        assert!(
            output.stdout.contains("dry run"),
            "dry-run output should say 'dry run': {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("Remote:") || output.stdout.contains("remote"),
            "dry-run output should mention remote: {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("Files to send:"),
            "dry-run output should mention file count: {}",
            output.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_remote_unreachable_fails_with_no_fallback_message() {
        let workspace = tempdir().unwrap();
        let project_dir = workspace.path().join("unreachableproject");
        fs::create_dir_all(&project_dir).unwrap();
        write_file(
            &project_dir.join("code.rs"),
            "fn example() -> bool { true }\n".repeat(10).as_str(),
        );

        let config_root = temp_config_root("remote-unreachable");
        let wing_name = "wing_unreachableproject";
        let remote_name = "hub";
        // Port 1 is not bindable — any connection will fail immediately.
        let server_url = "http://127.0.0.1:1";
        write_remote_cli_config(
            &config_root,
            remote_name,
            server_url,
            "unreachable-tok",
            wing_name,
            &project_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let result = run_cli(
            ["mine", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        );

        // Should fail through legacy CliOutput path with exit code 1.
        let output = result.expect("remote-only must return Ok(CliOutput), not Err");
        assert_eq!(output.exit_code, 1, "remote-only must exit code 1: {output:?}");
        assert!(
            output.stderr.contains("unreachable") && output.stderr.contains("local"),
            "error should mention unreachable and no local fallback: {}",
            output.stderr
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_combined_both_unreachable_returns_local_success() {
        let workspace = tempdir().unwrap();
        let project_dir = workspace.path().join("combinedproject");
        fs::create_dir_all(&project_dir).unwrap();
        write_file(
            &project_dir.join("code.rs"),
            "fn example() -> bool { true }\n".repeat(10).as_str(),
        );

        let config_root = temp_config_root("combined-unreachable");
        let context = CliContext::for_tests(config_root.clone());
        let palace_dir = config_root.join("palace");

        // init creates the local palace and writes default config.json.
        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        // Overwrite config.json with combined+both routing pointing at
        // an unreachable port.
        let remote_name = "hub";
        let server_url = "http://127.0.0.1:1";
        write_combined_cli_config(
            &config_root,
            remote_name,
            server_url,
            "combined-unreachable-tok",
            "wing_combinedproject",
            &palace_dir,
            &project_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let output = run_cli(
            ["mine", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        )
        .unwrap();

        // Must succeed locally despite remote being unreachable.
        assert_eq!(output.exit_code, 0, "combined mine failed: stderr={:?}", output.stderr);
        assert!(
            output.stdout.contains("Files ingested:"),
            "local ingestion must appear: {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("replication: failed") || output.stdout.contains("replication: skipped"),
            "remote replication failure must be reported: {}",
            output.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_combined_both_remote_success_reports_with_local() {
        const TOKEN: &str = "combined-success-tok-002";
        let workspace = tempdir().unwrap();

        // Server palace dir.
        let server_palace = workspace.path().join("server-palace");
        fs::create_dir_all(&server_palace).unwrap();
        let addr = spawn_test_server(server_palace.clone(), TOKEN);
        let server_url = format!("http://{addr}");

        // Project dir.
        let project_dir = workspace.path().join("myproject");
        fs::create_dir_all(&project_dir).unwrap();
        write_file(
            &project_dir.join("backend/auth.rs"),
            "Auth login flow keeps auth checks in the backend service.\n"
                .repeat(5)
                .as_str(),
        );
        write_file(
            &project_dir.join("docs/roadmap.md"),
            "Roadmap plan tracks the migration milestones.\n".repeat(5).as_str(),
        );

        // CLI config dir: init local palace, then overwrite with combined+both.
        let config_root = temp_config_root("combined-e2e");
        let context = CliContext::for_tests(config_root.clone());
        let palace_dir = config_root.join("palace");

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        let wing_name = "wing_myproject";
        let remote_name = "hub";
        write_combined_cli_config(
            &config_root,
            remote_name,
            &server_url,
            TOKEN,
            wing_name,
            &palace_dir,
            &project_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let output = run_cli(
            ["mine", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_eq!(output.exit_code, 0, "combined mine failed: stderr={:?}", output.stderr);
        assert!(
            output.stdout.contains("Files ingested:"),
            "must show local ingestion results: {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("replication: succeeded"),
            "remote replication must report success: {}",
            output.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_remote_only_unreachable_still_fails_legacy() {
        // Verify that a remote-only (not combined) route still fails through
        // the legacy error path when the remote is unreachable.
        let workspace = tempdir().unwrap();
        let project_dir = workspace.path().join("legacy-remote");
        fs::create_dir_all(&project_dir).unwrap();
        write_file(
            &project_dir.join("code.rs"),
            "fn example() -> bool { true }\n".repeat(10).as_str(),
        );

        let config_root = temp_config_root("legacy-remote-unreachable");
        let wing_name = "wing_legacyremote";
        let remote_name = "hub";
        let server_url = "http://127.0.0.1:1";
        write_remote_cli_config(
            &config_root,
            remote_name,
            server_url,
            "legacy-unreachable-tok",
            wing_name,
            &project_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let result = run_cli(
            ["mine", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        );

        // Must fail through legacy CliOutput path with exit code 1.
        let output = result.expect("legacy remote-only must return Ok(CliOutput), not Err");
        assert_eq!(output.exit_code, 1, "legacy remote-only must exit code 1: {output:?}");
        assert!(
            output.stderr.contains("unreachable") && output.stderr.contains("local"),
            "legacy error should mention unreachable and no fallback: {}",
            output.stderr
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_reindex_flag_accepted_and_plumbed() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("reindex-flag");
        let context = CliContext::for_tests(config_root.clone());

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        // First mine populates the palace.
        let first = run_cli(["mine", project_dir.to_str().unwrap()], &context, stub_provider).unwrap();
        assert_eq!(first.exit_code, 0);

        // Second mine without --reindex skips all three unchanged fixture files.
        let second = run_cli(["mine", project_dir.to_str().unwrap()], &context, stub_provider).unwrap();
        assert_eq!(second.exit_code, 0);
        assert!(
            second.stdout.contains("Files skipped unchanged: 3"),
            "expected all files skipped: {:?}",
            second.stdout
        );
        assert!(second.stdout.contains("Files ingested: 0"), "second run: {:?}", second.stdout);

        // --reindex forces re-ingestion of all three files despite unchanged hashes.
        let reindex = run_cli(
            ["mine", project_dir.to_str().unwrap(), "--reindex"],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(reindex.exit_code, 0);
        assert!(
            reindex.stdout.contains("Files skipped unchanged: 0"),
            "reindex run must skip nothing: {:?}",
            reindex.stdout
        );
        assert!(reindex.stdout.contains("Files ingested: 3"), "reindex output: {:?}", reindex.stdout);

        remove_dir_all_if_exists(&config_root);
    }

    /// Spawn a test server that deliberately delays the third request (info +
    /// first ingest batch are fast; the third is the second ingest batch) to
    /// exceed the client's 5-second timeout, causing a deterministic transport
    /// failure. Subsequent requests respond normally, so a retry works.
    fn spawn_self_destruct_server(
        palace_dir: PathBuf,
        token: &str,
    ) -> std::net::SocketAddr {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let token_dir = tempfile::tempdir().unwrap();
        let token_file = write_test_token_file(token_dir.path(), token);
        let config = remote_test_config(palace_dir, token_file.clone());

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let addr = rt.block_on(async {
            let tokens = mempalace_server::TokenRegistry::load(token_file).unwrap();
            let provider = mempalace_embeddings::DeterministicStubProvider::new(
                EmbeddingProfile::Balanced,
            );
            let router = mempalace_server::build_router(config, provider, tokens)
                .await
                .unwrap();

            let counter = Arc::new(AtomicUsize::new(0));
            let app = router.layer(axum::middleware::from_fn({
                let c = counter.clone();
                move |req: axum::extract::Request, next: axum::middleware::Next| {
                    let c = c.clone();
                    async move {
                        let prev = c.fetch_add(1, Ordering::SeqCst);
                        // prev=0: info handshake, prev=1: first ingest batch,
                        // prev=2: second ingest batch — sleep past client timeout.
                        if prev == 2 {
                            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                        }
                        next.run(req).await
                    }
                }
            }));

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                axum::serve(listener, app).await.ok();
            });
            addr
        });

        std::mem::forget(rt);
        std::mem::forget(token_dir);
        addr
    }

    #[test]
    fn mine_combined_both_partial_on_batch_transport_failure() {
        // Regression: when the first remote batch succeeds but a later batch
        // suffers a transport error, the output must say "partial" — never
        // "replication: succeeded".
        const TOKEN: &str = "partial-batch-tok-003";
        let workspace = tempdir().unwrap();

        // Server palace dir.
        let server_palace = workspace.path().join("server-palace");
        fs::create_dir_all(&server_palace).unwrap();
        let proxy_addr = spawn_self_destruct_server(server_palace.clone(), TOKEN);
        let proxy_url = format!("http://{proxy_addr}");

        // Project dir with enough files for 3 batches at batch-size 1.
        let project_dir = workspace.path().join("multi-batch-project");
        fs::create_dir_all(&project_dir).unwrap();
        for i in 0..3 {
            write_file(
                &project_dir.join(format!("file{i}.rs")),
                &format!("// File {i}\nfn example_{i}() -> bool {{ true }}\n").repeat(5),
            );
        }

        // CLI config dir.
        let config_root = temp_config_root("partial-batch-e2e");
        let context = CliContext::for_tests(config_root.clone());
        let palace_dir = config_root.join("palace");

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        let wing_name = "wing_multibatch";
        let remote_name = "hub";
        write_combined_cli_config(
            &config_root,
            remote_name,
            &proxy_url,
            TOKEN,
            wing_name,
            &palace_dir,
            &project_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let output = run_cli(
            [
                "mine",
                project_dir.to_str().unwrap(),
                "--batch-size",
                "1",
            ],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_eq!(output.exit_code, 0, "combined mine must exit 0: stderr={:?}", output.stderr);
        assert!(
            output.stdout.contains("Files ingested:"),
            "local ingestion must appear: {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("replication: partial"),
            "must report partial replication: {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("transport interrupted"),
            "must mention transport interruption: {}",
            output.stdout
        );
        assert!(
            !output.stdout.contains("replication: succeeded"),
            "must NOT claim full replication success: {}",
            output.stdout
        );
        assert!(
            !output.stdout.contains("some files had errors"),
            "must NOT claim file-level errors when the only issue was transport: {}",
            output.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_combined_both_retry_safe_replication() {
        // Regression: after a transport interruption mid-way through a combined
        // write:both mine, retrying the same project must not create duplicate
        // local drawers, and the remote must report previously-replicated files
        // as skipped_unchanged.
        const TOKEN: &str = "retry-safe-tok-004";
        let workspace = tempdir().unwrap();

        // Server palace dir — single server used for both mines.
        let server_palace = workspace.path().join("server-palace");
        fs::create_dir_all(&server_palace).unwrap();
        let proxy_addr = spawn_self_destruct_server(server_palace.clone(), TOKEN);
        let proxy_url = format!("http://{proxy_addr}");

        // Project dir with 3 files so batch-size 1 produces 3 batches.
        let project_dir = workspace.path().join("retry-project");
        fs::create_dir_all(&project_dir).unwrap();
        for i in 0..3 {
            write_file(
                &project_dir.join(format!("file{i}.rs")),
                &format!("// File {i}\nfn example_{i}() -> bool {{ true }}\n").repeat(5),
            );
        }

        // ── First mine (interrupted) ──────────────────────────────────────────
        let config_root = temp_config_root("retry-safe-e2e");
        let context = CliContext::for_tests(config_root.clone());
        let palace_dir = config_root.join("palace");

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        let wing_name = "wing_retryproject";
        let remote_name = "hub";
        write_combined_cli_config(
            &config_root,
            remote_name,
            &proxy_url,
            TOKEN,
            wing_name,
            &palace_dir,
            &project_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let first_output = run_cli(
            ["mine", project_dir.to_str().unwrap(), "--batch-size", "1"],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_eq!(first_output.exit_code, 0, "first mine must exit 0: stderr={:?}", first_output.stderr);
        assert!(first_output.stdout.contains("Files ingested: 3"), "local mine must ingest 3: {}",
            first_output.stdout);
        assert!(
            first_output.stdout.contains("replication: partial"),
            "first mine must report partial replication: {}",
            first_output.stdout
        );
        assert!(
            first_output.stdout.contains("transport interrupted"),
            "first mine must mention transport interruption: {}",
            first_output.stdout
        );

        // ── Second mine (retry — server is now past the slow delay) ───────────
        let second_output = run_cli(
            ["mine", project_dir.to_str().unwrap(), "--batch-size", "1"],
            &context,
            stub_provider,
        )
        .unwrap();

        assert_eq!(second_output.exit_code, 0, "second mine must exit 0: stderr={:?}", second_output.stderr);

        // Local mine re-ingests nothing because files are unchanged.
        assert!(
            second_output.stdout.contains("Files skipped unchanged: 3"),
            "retry must skip all locally-unchanged files: {}",
            second_output.stdout
        );
        assert!(
            second_output.stdout.contains("Files ingested: 0"),
            "retry must not re-ingest unchanged files: {}",
            second_output.stdout
        );

        // Remote should report the previously-replicated file as skipped_unchanged
        // and the remaining two as ingested.
        assert!(
            second_output.stdout.contains("replication: succeeded"),
            "second mine must report full replication success: {}",
            second_output.stdout
        );
        assert!(
            second_output.stdout.contains("Files ingested: 2"),
            "remote mine must ingest the two files that were not previously replicated: {}",
            second_output.stdout
        );

        // ── Verify no duplicates: local drawer count is unchanged after retry ──
        let status = run_cli(["status"], &context, stub_provider).unwrap();
        assert_eq!(status.exit_code, 0, "status failed: stderr={:?}", status.stderr);
        // The fixture has 3 files → 3 drawers after the first mine.
        // After retry there should still be 3 (no duplicates, nothing added).
        let drawer_count_line = status.stdout.lines().find(|l| l.contains("drawers")).unwrap_or("");
        assert!(
            drawer_count_line.contains("3 drawers"),
            "must have exactly 3 drawers after retry, not more (no duplicates): {}",
            status.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_combined_both_branch_uses_local_only() {
        // combined+both config with a healthy remote, but --branch forces
        // local-only execution. No remote replication must be attempted.
        const TOKEN: &str = "branch-local-tok-005";
        let workspace = tempdir().unwrap();

        let server_palace = workspace.path().join("server-palace");
        fs::create_dir_all(&server_palace).unwrap();
        let addr = spawn_test_server(server_palace.clone(), TOKEN);
        let server_url = format!("http://{addr}");

        let repo_dir = workspace.path().join("repo");
        fs::create_dir_all(&repo_dir).unwrap();
        let base_content = "fn base() -> i32 { 42 }\n".repeat(20);
        git_init_repo(
            &repo_dir,
            &[
                ("mempalace.yaml", "wing: branchtest\nrooms:\n  - name: general\n"),
                ("base.rs", &base_content),
                ("stable.rs", "fn stable() -> &str { \"hello\" }\n"),
            ],
        );

        // Create feature branch.
        let run_git = |args: &[&str]| {
            std::process::Command::new("git")
                .args(args)
                .current_dir(&repo_dir)
                .status()
                .unwrap();
        };
        run_git(&["checkout", "-b", "feature"]);
        let changed = "fn base() -> i32 { 99 }\n".repeat(20);
        fs::write(repo_dir.join("base.rs"), &changed).unwrap();

        let config_root = temp_config_root("branch-local");
        let context = CliContext::for_tests(config_root.clone());
        let palace_dir = config_root.join("palace");

        run_cli(["init", repo_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        let wing_name = "wing_branchtest";
        let remote_name = "hub";
        write_combined_cli_config(
            &config_root,
            remote_name,
            &server_url,
            TOKEN,
            wing_name,
            &palace_dir,
            &repo_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let output = run_cli(
            ["mine", repo_dir.to_str().unwrap(), "--branch"],
            &context,
            stub_provider,
        )
        .unwrap();

        // Must exit 0 and ingest the changed file locally.
        assert_eq!(
            output.exit_code, 0,
            "branch mine with combined+both config: stderr={:?}",
            output.stderr
        );
        assert!(
            output.stdout.contains("Files ingested: 1"),
            "must ingest the changed file: {}",
            output.stdout
        );

        // Must NOT contain any replication labels (branch forces local-only).
        assert!(
            !output.stdout.contains("replication:"),
            "branch mode must not attempt replication: {}",
            output.stdout
        );
        assert!(
            !output.stdout.contains("Remote replication:"),
            "branch mode must not show Remote replication: {}",
            output.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_combined_both_convos_uses_local_only() {
        // combined+both config with a healthy remote, but --mode convos forces
        // local-only execution. No remote replication must be attempted.
        const TOKEN: &str = "convos-local-tok-006";
        let workspace = tempdir().unwrap();

        let server_palace = workspace.path().join("server-palace");
        fs::create_dir_all(&server_palace).unwrap();
        let addr = spawn_test_server(server_palace.clone(), TOKEN);
        let server_url = format!("http://{addr}");

        let convo_dir = setup_convo_fixture(workspace.path());

        let config_root = temp_config_root("convos-local");
        let context = CliContext::for_tests(config_root.clone());
        let palace_dir = config_root.join("palace");

        // Write combined+both config manually (no init for convos).
        let wing_name = "wing_talks";
        let remote_name = "hub";
        write_combined_cli_config(
            &config_root,
            remote_name,
            &server_url,
            TOKEN,
            wing_name,
            &palace_dir,
            // The project_dir parameter in write_combined_cli_config writes
            // a mempalace.yaml. We'll use the convo dir for that.
            &convo_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let output = run_cli(
            [
                "--palace",
                workspace.path().join("convo-palace").to_str().unwrap(),
                "mine",
                convo_dir.to_str().unwrap(),
                "--mode",
                "convos",
                "--wing",
                "talks",
            ],
            &context,
            stub_provider,
        )
        .unwrap();

        // Must exit 0 and ingest conversation files.
        assert_eq!(
            output.exit_code, 0,
            "convos mine with combined+both config: stderr={:?}",
            output.stderr
        );
        assert!(
            output.stdout.contains("Files ingested:"),
            "must show local ingestion: {}",
            output.stdout
        );

        // Must NOT contain any replication labels (convos mode forces local-only).
        assert!(
            !output.stdout.contains("replication:"),
            "convos mode must not attempt replication: {}",
            output.stdout
        );
        assert!(
            !output.stdout.contains("Remote replication:"),
            "convos mode must not show Remote replication: {}",
            output.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_local_only_route_with_federation() {
        // Federation remotes are defined, but the wing is NOT mapped to any
        // remote — the route resolves to local-only. No remote replication
        // must be attempted.
        const TOKEN: &str = "local-route-tok-007";
        let workspace = tempdir().unwrap();

        // Start a server to prove we *could* replicate if configured.
        let server_palace = workspace.path().join("server-palace");
        fs::create_dir_all(&server_palace).unwrap();
        let addr = spawn_test_server(server_palace.clone(), TOKEN);
        let server_url = format!("http://{addr}");

        let project_dir = setup_project_fixture(workspace.path());

        let config_root = temp_config_root("local-route");
        let context = CliContext::for_tests(config_root.clone());
        let palace_dir = config_root.join("palace");

        // Write config with federation remotes defined but NO wing routing.
        fs::create_dir_all(&config_root).unwrap();
        let config_json = serde_json::json!({
            "version": 1,
            "palace_path": palace_dir.to_str().unwrap(),
            "federation": {
                "remotes": [{"name": "hub", "url": server_url, "token": TOKEN}],
                "wings": {}
            }
        });
        fs::write(
            config_root.join("config.json"),
            serde_json::to_string_pretty(&config_json).unwrap(),
        )
        .unwrap();

        // Write project config.
        let yaml = "wing: wing_project_alpha\nrooms:\n  - name: general\n    description: General files\n";
        fs::write(project_dir.join("mempalace.yaml"), yaml).unwrap();

        let context = CliContext::for_tests(config_root.clone());

        // First mine must succeed locally.
        let mine = run_cli(
            ["mine", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(mine.exit_code, 0, "local mine must succeed: stderr={:?}", mine.stderr);
        assert!(
            mine.stdout.contains("Files ingested: 3"),
            "must ingest all 3 fixture files: {}",
            mine.stdout
        );

        // Must NOT contain any replication or remote labels.
        assert!(
            !mine.stdout.contains("replication:"),
            "local-only route must not attempt replication: {}",
            mine.stdout
        );
        assert!(
            !mine.stdout.contains("Remote replication:"),
            "local-only route must not show Remote replication: {}",
            mine.stdout
        );
        assert!(
            !mine.stdout.contains("Remote:"),
            "local-only route must not mention Remote: {}",
            mine.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_combined_both_unsupported_remote() {
        // combined + write:both where the remote server does NOT advertise the
        // "ingest" capability — local mine must succeed, replication must be
        // reported as skipped, and exit code must be 0.
        const TOKEN: &str = "unsupported-remote-tok-008";
        let workspace = tempdir().unwrap();

        // Server without "ingest" capability.
        let addr = spawn_no_ingest_server(TOKEN);
        let server_url = format!("http://{addr}");

        let project_dir = workspace.path().join("no-ingest-project");
        fs::create_dir_all(&project_dir).unwrap();
        write_file(
            &project_dir.join("backend/auth.rs"),
            "Auth login flow keeps auth checks in the backend service.\n"
                .repeat(5)
                .as_str(),
        );
        write_file(
            &project_dir.join("docs/roadmap.md"),
            "Roadmap plan tracks the migration milestones.\n".repeat(5).as_str(),
        );

        let config_root = temp_config_root("unsupported-remote");
        let context = CliContext::for_tests(config_root.clone());
        let palace_dir = config_root.join("palace");

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        let wing_name = "wing_noingestproject";
        let remote_name = "hub";
        write_combined_cli_config(
            &config_root,
            remote_name,
            &server_url,
            TOKEN,
            wing_name,
            &palace_dir,
            &project_dir,
        );

        let context = CliContext::for_tests(config_root.clone());
        let output = run_cli(
            ["mine", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        )
        .unwrap();

        // Exit code 0 — local success even though remote is unsupported.
        assert_eq!(output.exit_code, 0, "combined mine failed: stderr={:?}", output.stderr);
        assert!(
            output.stdout.contains("Files ingested:"),
            "local ingestion must appear: {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("replication: skipped"),
            "must report replication as skipped: {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("does not support the ingest capability"),
            "must identify unsupported ingest capability: {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("Server capabilities: drawers, kg, changes, taxonomy"),
            "must list server capabilities: {}",
            output.stdout
        );
        assert!(
            output.stdout.contains("local mine completed successfully"),
            "must note local mine success: {}",
            output.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }

    #[test]
    fn mine_combined_both_diary_wing_uses_local_only() {
        // combined+both config with a healthy remote targeting wing_agents,
        // but the diary hard-override in route resolution forces local-only
        // execution. No remote replication must be attempted.
        const TOKEN: &str = "diary-wing-tok-009";
        let workspace = tempdir().unwrap();

        let server_palace = workspace.path().join("server-palace");
        fs::create_dir_all(&server_palace).unwrap();
        let addr = spawn_test_server(server_palace.clone(), TOKEN);
        let server_url = format!("http://{addr}");

        // Project dir with wing_agents as the wing.
        let project_dir = workspace.path().join("diary-project");
        fs::create_dir_all(&project_dir).unwrap();
        write_file(
            &project_dir.join("entry.md"),
            "Today I learned about federation routing.\n".repeat(5).as_str(),
        );

        let config_root = temp_config_root("diary-wing-local");
        let context = CliContext::for_tests(config_root.clone());
        let palace_dir = config_root.join("palace");

        run_cli(["init", project_dir.to_str().unwrap(), "--yes"], &context, stub_provider).unwrap();

        // Overwrite config with combined+both routing for wing_agents targeting
        // a healthy remote server. The diary hard-override in resolve_route
        // must force local-only regardless.
        let wing_name = "wing_agents";
        let remote_name = "hub";
        write_combined_cli_config(
            &config_root,
            remote_name,
            &server_url,
            TOKEN,
            wing_name,
            &palace_dir,
            &project_dir,
        );

        // Re-read project config to set wing_agents in mempalace.yaml.
        let yaml = "wing: wing_agents\nrooms:\n  - name: diary\n    description: Daily entries\n";
        fs::write(project_dir.join("mempalace.yaml"), yaml).unwrap();

        let context = CliContext::for_tests(config_root.clone());
        let output = run_cli(
            ["mine", project_dir.to_str().unwrap()],
            &context,
            stub_provider,
        )
        .unwrap();

        // Must exit 0 and ingest locally.
        assert_eq!(
            output.exit_code, 0,
            "diary-wing mine with combined+both config: stderr={:?}",
            output.stderr
        );
        assert!(
            output.stdout.contains("Files ingested:"),
            "must show local ingestion: {}",
            output.stdout
        );

        // Must NOT contain any replication labels (diary hard-override forces local-only).
        assert!(
            !output.stdout.contains("replication:"),
            "diary wing must not attempt replication: {}",
            output.stdout
        );
        assert!(
            !output.stdout.contains("Remote replication:"),
            "diary wing must not show Remote replication: {}",
            output.stdout
        );
        assert!(
            !output.stdout.contains("Remote:"),
            "diary wing must not mention Remote: {}",
            output.stdout
        );

        remove_dir_all_if_exists(&config_root);
    }
}
