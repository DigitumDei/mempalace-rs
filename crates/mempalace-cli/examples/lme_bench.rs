// LongMemEval in-process Rust benchmark.
//
// Loads the embedding model once, then runs all LongMemEval questions in-process
// (no subprocess per question). Measures quality (Recall@5, Recall@10, NDCG@10)
// and true single-load throughput.
//
// Usage:
//   cargo build --example lme_bench --release -p mempalace-cli
//   ./target/release/examples/lme_bench /path/to/longmemeval_s_cleaned.json
//   ./target/release/examples/lme_bench data.json --limit 20
#![allow(clippy::unwrap_used)]
#![allow(missing_docs)]

use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use mempalace_core::{EmbeddingProfile, SearchQuery};
use mempalace_embeddings::{FastembedProvider, FastembedProviderConfig};
use mempalace_ingest::{ProjectIngestRequest, ingest_project};
use mempalace_search::SearchRuntime;
use mempalace_storage::StorageEngine;
use serde_json::{Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: lme_bench <data.json> [--limit N] [--out DIR]");
        std::process::exit(1);
    }
    let data_path = &args[1];

    let mut limit = 0usize;
    let mut out_dir: Option<PathBuf> = None;
    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--limit" if i + 1 < args.len() => {
                i += 1;
                limit = args[i].parse()?;
            }
            "--out" if i + 1 < args.len() => {
                i += 1;
                out_dir = Some(PathBuf::from(&args[i]));
            }
            _ => {}
        }
        i += 1;
    }

    // Load dataset
    let raw = fs::read_to_string(data_path)?;
    let data: Value = serde_json::from_str(&raw)?;
    let entries_ref = data
        .as_array()
        .or_else(|| data.get("data").and_then(Value::as_array))
        .ok_or("dataset must be a JSON array or object with a 'data' key")?;
    let entries: &[Value] = if limit > 0 && limit < entries_ref.len() {
        &entries_ref[..limit]
    } else {
        entries_ref
    };
    let n = entries.len();

    // Build embedding provider — model loaded here, once.
    let cache_root = env::var_os("MEMPALACE_EMBED_CACHE")
        .map(PathBuf::from)
        .or_else(|| dirs::cache_dir().map(|d| d.join("mempalace").join("embeddings")))
        .ok_or("cannot determine cache root; set MEMPALACE_EMBED_CACHE")?;

    let allow_downloads = env::var("MEMPALACE_EMBED_ALLOW_DOWNLOADS")
        .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);

    let mut embed_config = FastembedProviderConfig::new(cache_root);
    embed_config.allow_downloads = allow_downloads;

    let profile = EmbeddingProfile::Balanced;

    eprintln!("Loading embedding model…");
    let provider = FastembedProvider::new(profile, embed_config).try_initialize()?;
    eprintln!("Model ready.\n");

    let mut search_rt = SearchRuntime::new(provider);

    // Single-thread tokio runtime: avoids Send bounds on FastembedProvider/TextEmbedding.
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;

    println!("LongMemEval in-process Rust — {n} questions");
    println!("  Embedding model loaded once; StorageEngine fresh per question\n");

    let mut r5_total = 0.0f64;
    let mut r10_total = 0.0f64;
    let mut ndcg_total = 0.0f64;
    let mut ingest_ms_total = 0u128;
    let mut search_ms_total = 0u128;
    let mut results_log: Vec<Value> = Vec::with_capacity(n);

    for (idx, entry) in entries.iter().enumerate() {
        let (corpus, corpus_ids) = build_corpus(entry);
        let question = entry["question"].as_str().unwrap_or("").to_owned();
        let correct_ids = get_correct_ids(entry);

        if corpus.is_empty() || question.trim().is_empty() {
            results_log.push(json!({
                "idx": idx,
                "question_id": entry.get("question_id").and_then(Value::as_u64).unwrap_or(idx as u64),
                "question_type": entry.get("question_type").and_then(Value::as_str),
                "correct_ids": correct_ids,
                "ranked_ids": [],
                "r5": 0.0, "r10": 0.0, "ndcg": 0.0,
                "ingest_ms": 0, "search_ms": 0,
            }));
            continue;
        }

        // Fresh tempdir per question — same isolation as the Python subprocess benchmark.
        let tmpdir = tempfile::TempDir::new()?;
        let sessions_dir = tmpdir.path().join("sessions");
        let palace_dir = tmpdir.path().join("palace");
        fs::create_dir_all(&sessions_dir)?;

        // Minimal project config (auto-skipped by ingest).
        fs::write(
            sessions_dir.join("mempalace.yaml"),
            "wing: bench\nrooms:\n  - name: general\n    description: Sessions\n    keywords: []\n",
        )?;

        let mut filename_to_id: HashMap<String, String> = HashMap::new();
        for (text, sess_id) in corpus.iter().zip(corpus_ids.iter()) {
            let fname = safe_filename(sess_id);
            fs::write(sessions_dir.join(&fname), text)?;
            filename_to_id.insert(fname, sess_id.clone());
        }

        // Ingest — timed from StorageEngine::open through ingest_project.
        let t_ingest = Instant::now();
        let engine = rt.block_on(StorageEngine::open(&palace_dir, profile))?;
        let ingest_req = ProjectIngestRequest::new(&sessions_dir);
        rt.block_on(ingest_project(&engine, search_rt.provider_mut(), &ingest_req))?;
        let ingest_ms = t_ingest.elapsed().as_millis();

        // Search.
        let t_search = Instant::now();
        let query =
            SearchQuery { text: question, wing: None, room: None, limit: 10, profile };
        let search_results =
            rt.block_on(search_rt.search(engine.drawer_store(), &query)).unwrap_or_default();
        let search_ms = t_search.elapsed().as_millis();

        // Map filenames back to original session IDs and pad unseen.
        let mut ranked_ids: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for result in &search_results {
            if let Some(sess_id) = filename_to_id.get(&result.source_file) {
                if seen.insert(sess_id.clone()) {
                    ranked_ids.push(sess_id.clone());
                }
            }
        }
        for cid in &corpus_ids {
            if seen.insert(cid.clone()) {
                ranked_ids.push(cid.clone());
            }
        }

        let r5 = recall_at_k(&ranked_ids, &correct_ids, 5);
        let r10 = recall_at_k(&ranked_ids, &correct_ids, 10);
        let ndcg = ndcg_at_k(&ranked_ids, &correct_ids, 10);

        r5_total += r5;
        r10_total += r10;
        ndcg_total += ndcg;
        ingest_ms_total += ingest_ms;
        search_ms_total += search_ms;

        results_log.push(json!({
            "idx": idx,
            "question_id": entry.get("question_id").and_then(Value::as_u64).unwrap_or(idx as u64),
            "question_type": entry.get("question_type").and_then(Value::as_str),
            "correct_ids": correct_ids,
            "ranked_ids": &ranked_ids[..ranked_ids.len().min(10)],
            "r5": r5, "r10": r10, "ndcg": ndcg,
            "ingest_ms": ingest_ms, "search_ms": search_ms,
        }));

        if (idx + 1) % 10 == 0 || idx + 1 == n {
            let done = idx + 1;
            println!(
                "  [{done:4}/{n}]  R@5={:.1}%  {ingest_ms}ms/{search_ms}ms",
                r5_total / done as f64 * 100.0,
            );
        }
    }

    // Summary
    println!("\n{}", "=".repeat(70));
    println!("  RESULTS SUMMARY");
    println!("{}\n", "=".repeat(70));
    if n > 0 {
        println!("  Rust (in-process, single model load):");
        println!("    Recall@5:       {:.3}  ({:.0}/{n})", r5_total / n as f64, r5_total);
        println!("    Recall@10:      {:.3}  ({:.0}/{n})", r10_total / n as f64, r10_total);
        println!("    NDCG@10:        {:.3}", ndcg_total / n as f64);
        println!(
            "    Avg ingest:     {:.0} ms/question",
            ingest_ms_total as f64 / n as f64
        );
        println!(
            "    Avg search:     {:.0} ms/question",
            search_ms_total as f64 / n as f64
        );
        println!(
            "    Total time:     {:.1}s",
            (ingest_ms_total + search_ms_total) as f64 / 1000.0
        );
    }

    // Save JSONL
    let ts = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "unknown".to_owned())
        .replace([':', '-'], "")
        .chars()
        .take(13)
        .collect::<String>(); // "20260427T2048"
    let filename = format!("results_lme_inprocess_{ts}.jsonl");
    let save_dir = out_dir.unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&save_dir)?;
    let out_path = save_dir.join(&filename);

    let mut out = String::new();
    for row in &results_log {
        out.push_str(&serde_json::to_string(row)?);
        out.push('\n');
    }
    fs::write(&out_path, &out)?;
    println!("\n  Results saved: {}", out_path.display());

    Ok(())
}

// =============================================================================
// Dataset helpers
// =============================================================================

fn build_corpus(entry: &Value) -> (Vec<String>, Vec<String>) {
    let sessions = entry["haystack_sessions"].as_array().cloned().unwrap_or_default();
    let session_ids = entry["haystack_session_ids"].as_array().cloned().unwrap_or_default();

    let mut corpus = Vec::new();
    let mut ids = Vec::new();

    for (session, id_val) in sessions.iter().zip(session_ids.iter()) {
        let sess_id = id_val.as_str().unwrap_or("").to_owned();
        if sess_id.is_empty() {
            continue;
        }
        let Some(session_arr) = session.as_array() else { continue };
        let user_turns: Vec<&str> = session_arr
            .iter()
            .filter(|turn| turn.get("role").and_then(Value::as_str) == Some("user"))
            .filter_map(|turn| turn.get("content").and_then(Value::as_str))
            .collect();

        if !user_turns.is_empty() {
            corpus.push(user_turns.join("\n"));
            ids.push(sess_id);
        }
    }

    (corpus, ids)
}

fn get_correct_ids(entry: &Value) -> Vec<String> {
    if let Some(arr) = entry.get("answer_session_ids").and_then(Value::as_array) {
        arr.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect()
    } else if let Some(s) = entry.get("answer_session_id").and_then(Value::as_str) {
        vec![s.to_owned()]
    } else {
        vec![]
    }
}

fn safe_filename(session_id: &str) -> String {
    let sanitized: String = session_id
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    format!("{sanitized}.txt")
}

// =============================================================================
// Metrics
// =============================================================================

fn recall_at_k(ranked: &[String], correct: &[String], k: usize) -> f64 {
    let top_k: HashSet<&str> = ranked.iter().take(k).map(String::as_str).collect();
    if correct.iter().any(|cid| top_k.contains(cid.as_str())) { 1.0 } else { 0.0 }
}

fn ndcg_at_k(ranked: &[String], correct: &[String], k: usize) -> f64 {
    let correct_set: HashSet<&str> = correct.iter().map(String::as_str).collect();
    let relevances: Vec<f64> = ranked
        .iter()
        .take(k)
        .map(|id| if correct_set.contains(id.as_str()) { 1.0 } else { 0.0 })
        .collect();

    let mut ideal = relevances.clone();
    ideal.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));

    let dcg: f64 =
        relevances.iter().enumerate().map(|(i, &r)| r / (i as f64 + 2.0).log2()).sum();
    let idcg: f64 =
        ideal.iter().enumerate().map(|(i, &r)| r / (i as f64 + 2.0).log2()).sum();

    if idcg > 0.0 { dcg / idcg } else { 0.0 }
}
