#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use mempalace_config::{
    ConfigFileV1, ConfigLoader, MempalaceConfig, ProjectRoomConfig, ResolvedPaths, build_runtime,
};
use mempalace_core::{EmbeddingProfile, RoomId, SearchQuery, WingId};
use mempalace_embeddings::{
    EmbeddingProvider, FastembedProvider, FastembedProviderConfig, env_flag, log_startup_validation,
};
use mempalace_ingest::{
    ConversationExtractMode, ConversationIngestRequest, IngestSummary, ProjectIngestRequest,
    ingest_conversations, ingest_project,
};
use mempalace_search::{Layer1Config, SearchRuntime, SearchRuntimePolicy, WakeUpRequest};
use mempalace_storage::{DrawerFilter, DrawerStore, StorageEngine, StorageLayout};
use serde_yaml::Mapping;
use tracing_subscriber::{EnvFilter, fmt};

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
    },
    /// Mine files into the palace.
    Mine {
        dir: PathBuf,
        #[arg(long, value_enum, default_value_t = CliMode::Projects)]
        mode: CliMode,
        #[arg(long)]
        wing: Option<String>,
        #[arg(long, default_value = "mempalace")]
        agent: String,
        #[arg(long, default_value_t = 0)]
        limit: usize,
        #[arg(long = "dry-run")]
        dry_run: bool,
        #[arg(long, value_enum, default_value_t = CliExtractMode::Exchange)]
        extract: CliExtractMode,
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

fn run_cli<I, T, F, P>(
    args: I,
    context: &CliContext,
    provider_factory: F,
) -> Result<CliOutput, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
    F: Fn(EmbeddingProfile, PathBuf) -> Result<P, Box<dyn std::error::Error>> + Copy,
    P: EmbeddingProvider,
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
    P: EmbeddingProvider,
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
    P: EmbeddingProvider,
    G: Fn(EmbeddingProfile, PathBuf) -> Result<Q, Box<dyn std::error::Error>>,
    Q: EmbeddingProvider,
{
    let Some(command) = cli.command else {
        return Ok(CliOutput::success(render_help()));
    };
    match command {
        Commands::Init { dir, yes } => {
            execute_init(&dir, yes, cli.palace.as_deref(), context, validation_provider_factory)
        }
        Commands::Mine { dir, mode, wing, agent, limit, dry_run, extract } => execute_mine(
            &dir,
            mode,
            wing,
            agent,
            limit,
            dry_run,
            extract,
            cli.palace.as_deref(),
            context,
            provider_factory,
        ),
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
        Commands::Split { .. } => Ok(deferred_command("split")),
        Commands::Compress { .. } => Ok(deferred_command("compress")),
    }
}

fn execute_init<F, P>(
    dir: &Path,
    yes: bool,
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

    if config_path.exists() && !yes {
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

    write_project_config(&config_path, &project_dir, &detection.rooms, yes).map_err(io_error)?;

    let mut lines = vec![
        format!("\n{}", "=".repeat(INIT_HEADER_WIDTH)),
        "  MemPalace Init — Local setup".to_owned(),
        "=".repeat(INIT_HEADER_WIDTH),
        String::new(),
        format!("  WING: {}", wing_name_for_dir(&project_dir)),
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
        format!("  Config saved: {}", config_path.display()),
        format!("  Palace path: {}", config.palace_path.display()),
        format!("  Startup validation: {}", validation.status),
        format!("  Global config: {}", runtime_paths.config_file.display()),
        "  Next step:".to_owned(),
        format!("    mempalace-cli mine {}", project_dir.display()),
        format!("\n{}\n", "=".repeat(INIT_HEADER_WIDTH)),
    ]);

    Ok(CliOutput::success(lines.join("\n")))
}

fn execute_mine<F, P>(
    dir: &Path,
    mode: CliMode,
    wing: Option<String>,
    agent: String,
    limit: usize,
    dry_run: bool,
    extract: CliExtractMode,
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
    let use_temp_storage = dry_run && !palace_exists(&config.palace_path);
    let runtime = build_runtime(&config).map_err(runtime_error)?;
    let dry_run_storage =
        if use_temp_storage { Some(tempfile::tempdir().map_err(io_error)?) } else { None };
    let storage_root =
        dry_run_storage.as_ref().map(|dir| dir.path()).unwrap_or(config.palace_path.as_path());
    let engine = runtime
        .block_on(StorageEngine::open(storage_root, config.embedding_profile))
        .map_err(storage_error)?;
    let mut provider = provider_factory(config.embedding_profile, default_embedding_cache_dir())
        .map_err(provider_error)?;

    let summary = match mode {
        CliMode::Projects => runtime
            .block_on(ingest_project(
                &engine,
                &mut provider,
                &ProjectIngestRequest {
                    project_dir: source_dir.clone(),
                    wing,
                    agent,
                    limit: if limit == 0 { None } else { Some(limit) },
                    dry_run,
                    max_embed_batch_size: config
                        .low_cpu
                        .enabled
                        .then_some(config.low_cpu.effective_ingest_batch_size()),
                },
            ))
            .map_err(ingest_error)?,
        CliMode::Convos => runtime
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
                    max_embed_batch_size: config
                        .low_cpu
                        .enabled
                        .then_some(config.low_cpu.effective_ingest_batch_size()),
                },
            ))
            .map_err(ingest_error)?,
    };

    Ok(CliOutput::success(render_mine_summary(
        mode,
        &source_dir,
        &config.palace_path,
        dry_run,
        &summary,
    )))
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
    let mut provider = provider_factory(config.embedding_profile, default_embedding_cache_dir())
        .map_err(provider_error)?;
    let mut search = SearchRuntime::with_policy(
        provider,
        SearchRuntimePolicy { rerank_enabled: config.low_cpu.effective_rerank_enabled() },
    );

    let wing_id = wing.as_deref().map(WingId::new).transpose().map_err(id_error)?;
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
                wing: wing.as_deref().map(WingId::new).transpose().map_err(id_error)?,
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

    [
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
        format!("{}\n", "=".repeat(SEARCH_HEADER_WIDTH)),
    ]
    .join("\n")
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
    rooms: &[ProjectRoomConfig],
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
        serde_yaml::Value::String(wing_name_for_dir(project_dir)),
    );
    root.insert(
        serde_yaml::Value::String("rooms".to_owned()),
        serde_yaml::to_value(rooms).map_err(|error| std::io::Error::other(error.to_string()))?,
    );

    fs::write(
        config_path,
        serde_yaml::to_string(&root).map_err(|error| std::io::Error::other(error.to_string()))?,
    )
}

fn wing_name_for_dir(project_dir: &Path) -> String {
    project_dir
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("project")
        .to_lowercase()
        .replace('-', "_")
        .replace(' ', "_")
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
    fn init_creates_project_config_and_global_config_without_yes_on_first_run() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("init");
        let context = CliContext::for_tests(config_root.clone());

        let output =
            run_cli(["init", project_dir.to_str().unwrap()], &context, stub_provider).unwrap();

        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("MemPalace Init"));
        assert!(project_dir.join("mempalace.yaml").exists());
        assert!(config_root.join("config.json").exists());
        assert!(config_root.join("palace").exists());
        fs::remove_dir_all(config_root).unwrap();
    }

    #[test]
    fn init_requires_yes_only_when_overwriting_existing_project_config() {
        let workspace = tempdir().unwrap();
        let project_dir = setup_project_fixture(workspace.path());
        let config_root = temp_config_root("init-yes");
        let context = CliContext::for_tests(config_root.clone());
        let existing = "wing: preserved\nrooms:\n  - name: archive\n    description: Keep me\n";
        write_file(&project_dir.join("mempalace.yaml"), existing);

        let output =
            run_cli(["init", project_dir.to_str().unwrap()], &context, stub_provider).unwrap();
        assert_eq!(output.exit_code, 1);
        assert!(output.stderr.contains("with `--yes` to overwrite it"));
        assert_eq!(fs::read_to_string(project_dir.join("mempalace.yaml")).unwrap(), existing);

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
        assert!(status.stdout.contains("WING: project_alpha"));

        let wake_up = run_cli(["wake-up"], &context, stub_provider).unwrap();
        assert_eq!(wake_up.exit_code, 0);
        assert!(wake_up.stdout.contains("Wake-up text"));
        assert!(wake_up.stdout.contains("ESSENTIAL STORY"));

        fs::remove_dir_all(config_root).unwrap();
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
        assert!(status.stdout.contains("WING: overridewing"));
        assert!(!status.stdout.contains("WING: project_alpha"));
        fs::remove_dir_all(config_root).unwrap();
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
            ["search", "reconciliation", "--wing", "project_beta"],
            &context,
            stub_provider,
        )
        .unwrap();
        assert_eq!(wing_search.exit_code, 0);
        assert!(wing_search.stdout.contains("project_beta"));
        assert!(!wing_search.stdout.contains("project_alpha"));

        let room_search =
            run_cli(["search", "roadmap", "--room", "planning"], &context, stub_provider).unwrap();
        assert_eq!(room_search.exit_code, 0);
        assert!(room_search.stdout.contains("planning"));

        let wake_up =
            run_cli(["wake-up", "--wing", "project_beta"], &context, stub_provider).unwrap();
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
}
