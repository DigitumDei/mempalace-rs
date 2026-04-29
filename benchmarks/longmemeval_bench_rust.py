#!/usr/bin/env python3
"""
LongMemEval: Python vs Rust retrieval comparison benchmark.

Measures retrieval quality (Recall@5, Recall@10, NDCG@10) and speed
(ingest + search wall-clock time) for both implementations on the same
LongMemEval dataset.

Both implementations use the same embedding model (all-MiniLM-L6-v2) and the
same text representation (user turns concatenated per session), so quality
differences reflect storage/retrieval implementation differences.

Usage:
    python benchmarks/longmemeval_bench_rust.py /path/to/longmemeval_s_cleaned.json
    python benchmarks/longmemeval_bench_rust.py data.json --limit 20
    python benchmarks/longmemeval_bench_rust.py data.json --rust-only
    python benchmarks/longmemeval_bench_rust.py data.json --python-only
    python benchmarks/longmemeval_bench_rust.py data.json --rust-binary /path/to/mempalace-cli

Data:
    curl -fsSL -o /tmp/longmemeval_s_cleaned.json \\
      https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main/longmemeval_s_cleaned.json
"""

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import chromadb


# =============================================================================
# METRICS
# =============================================================================


def recall_at_k(ranked_ids, correct_ids, k):
    """Fraction of questions where any correct session is in top-k."""
    top_k = set(ranked_ids[:k])
    return float(any(cid in top_k for cid in correct_ids))


def ndcg_at_k(ranked_ids, correct_ids, k):
    """Normalized DCG at k."""
    correct = set(correct_ids)
    relevances = [1.0 if rid in correct else 0.0 for rid in ranked_ids[:k]]

    def dcg(rels):
        return sum(r / math.log2(i + 2) for i, r in enumerate(rels))

    ideal = [1.0] * min(len(correct), k)
    idcg = dcg(ideal)
    return dcg(relevances) / idcg if idcg > 0 else 0.0


# =============================================================================
# PYTHON RETRIEVER (ChromaDB in-memory, raw mode — matches existing baseline)
# =============================================================================

_bench_client = chromadb.EphemeralClient()


def _fresh_collection():
    try:
        _bench_client.delete_collection("bench")
    except Exception:
        pass
    return _bench_client.create_collection("bench")


def build_corpus(entry):
    """Extract user-turn text per session from a LongMemEval entry."""
    sessions = entry["haystack_sessions"]
    session_ids = entry["haystack_session_ids"]
    corpus = []
    corpus_ids = []
    for session, sess_id in zip(sessions, session_ids):
        user_turns = [t["content"] for t in session if t["role"] == "user"]
        if user_turns:
            corpus.append("\n".join(user_turns))
            corpus_ids.append(sess_id)
    return corpus, corpus_ids


def python_query(entry, n_results=10):
    """Run a single question through the Python (ChromaDB) pipeline.

    Returns (ranked_session_ids, corpus_ids, ingest_secs, search_secs).
    """
    corpus, corpus_ids = build_corpus(entry)
    if not corpus:
        return [], corpus_ids, 0.0, 0.0

    t0 = time.perf_counter()
    col = _fresh_collection()
    col.add(
        documents=corpus,
        ids=[f"doc_{i}" for i in range(len(corpus))],
        metadatas=[{"corpus_id": cid} for cid in corpus_ids],
    )
    ingest_secs = time.perf_counter() - t0

    t1 = time.perf_counter()
    results = col.query(
        query_texts=[entry["question"]],
        n_results=min(n_results, len(corpus)),
        include=["metadatas"],
    )
    search_secs = time.perf_counter() - t1

    ranked_ids = [m["corpus_id"] for m in results["metadatas"][0]]
    # Pad with unseen IDs so ranking covers the full corpus
    seen = set(ranked_ids)
    for cid in corpus_ids:
        if cid not in seen:
            ranked_ids.append(cid)

    return ranked_ids, corpus_ids, ingest_secs, search_secs


# =============================================================================
# RUST RETRIEVER (mempalace-cli subprocess)
# =============================================================================


def _safe_filename(session_id: str) -> str:
    """Sanitize a session ID for use as a filename without collisions."""
    safe = re.sub(r"[^\w\-]", "_", session_id)[:32].strip("_") or "session"
    digest = hashlib.md5(session_id.encode("utf-8")).hexdigest()[:8]
    return f"{safe}_{digest}.txt"


def rust_query(entry, rust_binary: str, n_results=10):
    """Run a single question through the Rust CLI pipeline.

    Creates a fresh temp palace per question (same isolation as Python benchmark).
    Returns (ranked_session_ids, corpus_ids, ingest_secs, search_secs).
    """
    corpus, corpus_ids = build_corpus(entry)
    if not corpus:
        return [], corpus_ids, 0.0, 0.0

    with tempfile.TemporaryDirectory(prefix="mempalace_bench_") as tmpdir:
        sessions_dir = Path(tmpdir) / "sessions"
        palace_dir = Path(tmpdir) / "palace"
        sessions_dir.mkdir()

        # Minimal project config so the CLI doesn't require `init` first
        (sessions_dir / "mempalace.yaml").write_text(
            "wing: bench\nrooms:\n  - name: general\n    description: Sessions\n    keywords: []\n",
            encoding="utf-8",
        )

        # Map sanitized filename -> original session ID
        filename_to_id = {}
        for text, sess_id in zip(corpus, corpus_ids):
            fname = _safe_filename(sess_id)
            (sessions_dir / fname).write_text(text, encoding="utf-8")
            filename_to_id[fname] = sess_id

        env = {**os.environ, "MEMPALACE_EMBED_ALLOW_DOWNLOADS": "1"}

        # Ingest
        t0 = time.perf_counter()
        try:
            subprocess.run(
                [
                    rust_binary,
                    "--palace",
                    str(palace_dir),
                    "mine",
                    str(sessions_dir),
                    "--mode",
                    "projects",
                ],
                capture_output=True,
                check=True,
                env=env,
            )
        except subprocess.CalledProcessError as exc:
            print(f"  [rust] mine failed: {exc.stderr.decode()[:200]}", file=sys.stderr)
            return [], corpus_ids, 0.0, 0.0
        ingest_secs = time.perf_counter() - t0

        # Search
        t1 = time.perf_counter()
        try:
            result = subprocess.run(
                [
                    rust_binary,
                    "--palace",
                    str(palace_dir),
                    "search",
                    entry["question"],
                    "--results",
                    str(n_results),
                ],
                capture_output=True,
                text=True,
                check=True,
                env=env,
            )
        except subprocess.CalledProcessError as exc:
            print(f"  [rust] search failed: {exc.stderr[:200]}", file=sys.stderr)
            return [], corpus_ids, ingest_secs, 0.0
        search_secs = time.perf_counter() - t1

        # Parse result metadata from CLI output. Only accept Source lines in the
        # fixed metadata position immediately after a result header, so session
        # content containing "Source:" cannot be mistaken for a retrieval hit.
        ranked_ids = []
        seen = set()
        expecting_source = False
        for line in result.stdout.splitlines():
            if re.match(r"^\s+\[\d+\]\s+\S+\s+/\s+\S+\s*$", line):
                expecting_source = True
                continue

            if expecting_source and line.startswith("      Source: "):
                expecting_source = False
                stripped = line.strip()
                fname = stripped.split(":", 1)[1].strip()
                sess_id = filename_to_id.get(fname)
                if sess_id and sess_id not in seen:
                    ranked_ids.append(sess_id)
                    seen.add(sess_id)
                continue

            if line.strip():
                expecting_source = False

        # Pad with unseen IDs
        for cid in corpus_ids:
            if cid not in seen:
                ranked_ids.append(cid)
                seen.add(cid)

        return ranked_ids, corpus_ids, ingest_secs, search_secs


# =============================================================================
# BENCHMARK RUNNER
# =============================================================================


def run_benchmark(data_path: str, args):
    with open(data_path, encoding="utf-8") as f:
        dataset = json.load(f)

    entries = dataset if isinstance(dataset, list) else dataset.get("data", [])
    if args.limit > 0:
        entries = entries[: args.limit]

    n = len(entries)
    print(f"\nLongMemEval Python vs Rust — {n} questions")
    print("  Python:  ChromaDB in-memory, raw user turns")
    print(f"  Rust:    {args.rust_binary}")
    print()

    py_r5 = py_r10 = py_ndcg = 0.0
    py_ingest_total = py_search_total = 0.0
    rs_r5 = rs_r10 = rs_ndcg = 0.0
    rs_ingest_total = rs_search_total = 0.0

    results_log = []

    for idx, entry in enumerate(entries):
        qtype = entry.get("question_type", "unknown")
        correct_ids = entry.get("answer_session_ids") or entry.get("answer_session_id", [])
        if isinstance(correct_ids, str):
            correct_ids = [correct_ids]

        # ── Python ────────────────────────────────────────────────────────────
        py_ranked, corpus_ids, py_ing, py_srch = ([], [], 0.0, 0.0)
        if not args.rust_only:
            py_ranked, corpus_ids, py_ing, py_srch = python_query(entry, n_results=10)
            py_r5 += recall_at_k(py_ranked, correct_ids, 5)
            py_r10 += recall_at_k(py_ranked, correct_ids, 10)
            py_ndcg += ndcg_at_k(py_ranked, correct_ids, 10)
            py_ingest_total += py_ing
            py_search_total += py_srch

        # ── Rust ──────────────────────────────────────────────────────────────
        rs_ranked, _, rs_ing, rs_srch = ([], [], 0.0, 0.0)
        if not args.python_only:
            rs_ranked, _, rs_ing, rs_srch = rust_query(entry, args.rust_binary, n_results=10)
            rs_r5 += recall_at_k(rs_ranked, correct_ids, 5)
            rs_r10 += recall_at_k(rs_ranked, correct_ids, 10)
            rs_ndcg += ndcg_at_k(rs_ranked, correct_ids, 10)
            rs_ingest_total += rs_ing
            rs_search_total += rs_srch

        results_log.append(
            {
                "idx": idx,
                "question_id": entry.get("question_id", idx),
                "question_type": qtype,
                "correct_ids": correct_ids,
                "py_ranked_ids": py_ranked[:10],
                "rs_ranked_ids": rs_ranked[:10],
                "py_r5": recall_at_k(py_ranked, correct_ids, 5) if not args.rust_only else None,
                "rs_r5": recall_at_k(rs_ranked, correct_ids, 5) if not args.python_only else None,
                "py_ingest_ms": round(py_ing * 1000, 1),
                "rs_ingest_ms": round(rs_ing * 1000, 1),
                "py_search_ms": round(py_srch * 1000, 1),
                "rs_search_ms": round(rs_srch * 1000, 1),
            }
        )

        # Progress every 10 questions
        if (idx + 1) % 10 == 0 or idx == n - 1:
            done = idx + 1
            py_pct = f"{py_r5 / done:.1%}" if not args.rust_only else "—"
            rs_pct = f"{rs_r5 / done:.1%}" if not args.python_only else "—"
            print(
                f"  [{done:4d}/{n}]  Py R@5={py_pct}  Rs R@5={rs_pct}"
                f"  Py {py_ing * 1000:.0f}ms/{py_srch * 1000:.0f}ms"
                f"  Rs {rs_ing * 1000:.0f}ms/{rs_srch * 1000:.0f}ms"
            )

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70)
    print()

    if not args.rust_only:
        print("  Python (ChromaDB):")
        print(f"    Recall@5:       {py_r5 / n:.3f}  ({py_r5:.0f}/{n})")
        print(f"    Recall@10:      {py_r10 / n:.3f}  ({py_r10:.0f}/{n})")
        print(f"    NDCG@10:        {py_ndcg / n:.3f}")
        print(f"    Avg ingest:     {py_ingest_total / n * 1000:.0f} ms/question")
        print(f"    Avg search:     {py_search_total / n * 1000:.0f} ms/question")
        print(f"    Total time:     {py_ingest_total + py_search_total:.1f}s")
        print()

    if not args.python_only:
        print("  Rust (mempalace-cli):")
        print(f"    Recall@5:       {rs_r5 / n:.3f}  ({rs_r5:.0f}/{n})")
        print(f"    Recall@10:      {rs_r10 / n:.3f}  ({rs_r10:.0f}/{n})")
        print(f"    NDCG@10:        {rs_ndcg / n:.3f}")
        print(f"    Avg ingest:     {rs_ingest_total / n * 1000:.0f} ms/question")
        print(f"    Avg search:     {rs_search_total / n * 1000:.0f} ms/question")
        print(f"    Total time:     {rs_ingest_total + rs_search_total:.1f}s")
        print()

    if not args.rust_only and not args.python_only:
        print("  Quality delta (Rust - Python):")
        print(f"    ΔRecall@5:      {(rs_r5 - py_r5) / n:+.3f}")
        print(f"    ΔRecall@10:     {(rs_r10 - py_r10) / n:+.3f}")
        print(f"    ΔNDCG@10:       {(rs_ndcg - py_ndcg) / n:+.3f}")
        print()
        print("  Speed delta (Rust / Python — lower is faster for Rust):")
        if py_ingest_total > 0:
            print(f"    Ingest ratio:   {rs_ingest_total / py_ingest_total:.2f}x")
        if py_search_total > 0:
            print(f"    Search ratio:   {rs_search_total / py_search_total:.2f}x")
        print()

    # ── Save results ──────────────────────────────────────────────────────────
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    out_path = Path(__file__).parent / f"results_lme_rust_comparison_{timestamp}.jsonl"
    with open(out_path, "w", encoding="utf-8") as f:
        for row in results_log:
            f.write(json.dumps(row) + "\n")
    print(f"  Results saved: {out_path}")
    print()


# =============================================================================
# MAIN
# =============================================================================

_DEFAULT_BINARY = str(
    Path(__file__).parent.parent / "mempalace-rs" / "target" / "release" / "mempalace-cli"
)


def main():
    parser = argparse.ArgumentParser(description="LongMemEval Python vs Rust benchmark")
    parser.add_argument("data", help="Path to longmemeval_s_cleaned.json")
    parser.add_argument("--limit", type=int, default=0, help="Number of questions to run (0 = all)")
    parser.add_argument(
        "--rust-binary", default=_DEFAULT_BINARY, help="Path to mempalace-cli binary"
    )
    parser.add_argument("--rust-only", action="store_true", help="Skip Python baseline")
    parser.add_argument("--python-only", action="store_true", help="Skip Rust benchmark")
    args = parser.parse_args()

    if not Path(args.data).exists():
        sys.exit(f"Data file not found: {args.data}")

    if not args.python_only:
        if not Path(args.rust_binary).exists():
            sys.exit(
                f"Rust binary not found: {args.rust_binary}\n"
                f"Build it with: cargo build --release -p mempalace-cli\n"
                f"Or pass --rust-binary /path/to/mempalace-cli"
            )
        print(
            f"Rust binary: {args.rust_binary} ({Path(args.rust_binary).stat().st_size // 1_000_000}MB)"
        )

    run_benchmark(args.data, args)


if __name__ == "__main__":
    main()
