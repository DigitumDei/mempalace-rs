use std::env;
use std::path::PathBuf;

use agentpalace_core::EmbeddingProfile;
use agentpalace_embeddings::{
    EmbeddingBenchmark, EmbeddingRequest, FastembedProvider, FastembedProviderConfig, env_flag,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache_root =
        env::var_os("AGENTPALACE_EMBED_CACHE").map(PathBuf::from).unwrap_or_else(default_cache_root);
    let allow_downloads = env_flag("AGENTPALACE_EMBED_ALLOW_DOWNLOADS");
    let profile = agentpalace_core::env_var("AGENTPALACE_EMBED_PROFILE")
        .ok()
        .as_deref()
        .unwrap_or("balanced")
        .parse::<EmbeddingProfile>()?;
    let iterations = agentpalace_core::env_var("AGENTPALACE_EMBED_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(15);
    let request = EmbeddingRequest::new(vec![
        "AgentPalace benchmark query for warm-path embedding latency.".to_owned(),
    ])?;

    let mut config = FastembedProviderConfig::new(cache_root);
    config.allow_downloads = allow_downloads;

    let mut provider = FastembedProvider::new(profile, config).try_initialize()?;
    let benchmark =
        EmbeddingBenchmark::measure_with_warmup(&mut provider, &request, 3, iterations)?;

    if let Some(p95) = benchmark.p95_millis() {
        println!("profile={} p95_ms={p95:.2}", profile.as_str());
    }

    Ok(())
}

fn default_cache_root() -> PathBuf {
    PathBuf::from(".cache").join("agentpalace").join("embeddings")
}
