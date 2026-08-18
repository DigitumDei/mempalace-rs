#![allow(missing_docs)]

use std::ffi::OsString;

use mempalace_config::{ConfigLoader, build_runtime};
use mempalace_embeddings::env_flag;
use mempalace_mcp::{
    DeterministicStubProvider, McpServer, configured_lineage_id_from_env, default_provider,
    serve_transport,
};
use tokio::io::{self, BufReader};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(output) = early_output(std::env::args_os().skip(1)) {
        print!("{output}");
        return Ok(());
    }

    let config = ConfigLoader::load_with_env(None)?;
    let lineage_id = configured_lineage_id_from_env()?;
    build_runtime(&config)?.block_on(async move {
        if env_flag("MEMPALACE_STUB_EMBEDDINGS") {
            let server = McpServer::from_parts_with_lineage(
                config.clone(),
                DeterministicStubProvider::new(config.embedding_profile),
                lineage_id,
            )
            .await?;
            return serve_transport(&server, BufReader::new(io::stdin()), io::stdout()).await;
        }

        let server = McpServer::from_parts_with_lineage(
            config.clone(),
            default_provider(config.embedding_profile)?,
            lineage_id,
        )
        .await?;
        serve_transport(&server, BufReader::new(io::stdin()), io::stdout()).await
    })
}

fn early_output<I, S>(args: I) -> Option<String>
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    let mut saw_help = false;
    let mut saw_version = false;

    for arg in args.into_iter().map(Into::into) {
        match arg.to_str() {
            Some("--help") | Some("-h") => saw_help = true,
            Some("--version") | Some("-V") => saw_version = true,
            _ => {}
        }
    }

    if saw_help {
        Some(help_text().to_owned())
    } else if saw_version {
        Some(version_text())
    } else {
        None
    }
}

fn help_text() -> &'static str {
    concat!(
        "MemPalace MCP stdio server\n\n",
        "Usage: mempalace-mcp\n\n",
        "Options:\n",
        "  -h, --help     Print help\n",
        "  -V, --version  Print version\n",
        "\nEnvironment:\n",
        "  MEMPALACE_LINEAGE_ID  Bind identity wake-up; missing IDs fall back to the palace default\n",
    )
}

fn version_text() -> String {
    format!("mempalace-mcp {}\n", mempalace_core::BUILD_VERSION)
}

#[cfg(test)]
mod tests {
    use super::{early_output, help_text, version_text};

    #[test]
    fn help_and_version_short_circuit_before_startup() {
        assert_eq!(early_output(["--help"]), Some(help_text().to_owned()));
        assert_eq!(early_output(["-h"]), Some(help_text().to_owned()));
        assert_eq!(early_output(["--version"]), Some(version_text()));
        assert_eq!(early_output(["-V"]), Some(version_text()));
        assert_eq!(early_output(["--help", "--version"]), Some(help_text().to_owned()));
        assert_eq!(early_output(["--version", "--help"]), Some(help_text().to_owned()));
        assert_eq!(early_output(["--version", "--verbose"]), Some(version_text()));
        assert_eq!(early_output(std::iter::empty::<&str>()), None);
        assert_eq!(early_output(["--unknown"]), None);
    }
}
