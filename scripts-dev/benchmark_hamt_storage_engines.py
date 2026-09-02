#!/usr/bin/env python
"""Benchmarks HAMT node storage: Postgres (state_hamt_nodes, the existing
always-on SQL mirror) vs. fjall (rust/src/fjall_engine.rs).

Both are seeded with the same synthetic dataset -- 32-byte content-addressed
keys, 512-byte node payloads, uniformly random -- to match the real workload
(see rust/src/state_hamt.rs). Sweeps corpus size to see whether read/write
cost holds flat (as a log-N/cached index would predict) or grows, and
separately measures write cost: both bulk-load throughput (proxy for
write-amplification/compaction/index-maintenance cost) and small-batch
"commit" latency (5 nodes, the size of one real state-group publish) at
each corpus size, since a live write happening on an already-large,
already-hot dataset is the actual production condition -- not an empty table.

Usage:
    eval "$(scripts-dev/start_test_postgres.sh)"
    python3 scripts-dev/benchmark_hamt_storage_engines.py
"""

from __future__ import annotations

import os
import random
import shutil
import statistics
import tempfile
import time
from typing import Callable

import psycopg2
import psycopg2.extras

from synapse.synapse_rust import fjall_engine

NODE_SIZE = 512
CUMULATIVE_SIZES = (200_000, 1_000_000, 1_500_000)
READ_BATCH_SIZE = 100
READ_ITERATIONS = 200
COMMIT_BATCH_SIZE = 5  # typical single state-group publish
COMMIT_ITERATIONS = 200


def rand_rows(rng: random.Random, n: int) -> list[tuple[bytes, bytes]]:
    return [(rng.randbytes(32), rng.randbytes(NODE_SIZE)) for _ in range(n)]


def percentiles(samples: list[float]) -> tuple[float, float]:
    samples = sorted(samples)
    p50 = statistics.median(samples) * 1e6
    p99 = samples[int(len(samples) * 0.99)] * 1e6
    return p50, p99


def bench_reads(
    name: str,
    size: int,
    batch_fetch: "Callable[[list[bytes]], object]",
    keys_pool: list[bytes],
) -> None:
    rng = random.Random(1)
    samples = []
    for _ in range(READ_ITERATIONS):
        batch = rng.sample(keys_pool, READ_BATCH_SIZE)
        start = time.perf_counter()
        batch_fetch(batch)
        samples.append(time.perf_counter() - start)
    p50, p99 = percentiles(samples)
    print(
        f"{name:<10} n={size:>9,}  read(batch={READ_BATCH_SIZE:<3}) "
        f"p50={p50:8.1f}us  p99={p99:8.1f}us"
    )


def bench_commits(
    name: str,
    size: int,
    commit_write: "Callable[[list[tuple[bytes, bytes]]], object]",
    rng: random.Random,
) -> None:
    samples = []
    for _ in range(COMMIT_ITERATIONS):
        rows = rand_rows(rng, COMMIT_BATCH_SIZE)
        start = time.perf_counter()
        commit_write(rows)
        samples.append(time.perf_counter() - start)
    p50, p99 = percentiles(samples)
    print(
        f"{name:<10} n={size:>9,}  commit(batch={COMMIT_BATCH_SIZE:<3}) "
        f"p50={p50:8.1f}us  p99={p99:8.1f}us"
    )


def run_postgres() -> None:
    host = os.environ.get("SYNAPSE_POSTGRES_HOST", "/tmp/synapse-pgtest")
    port = int(os.environ.get("SYNAPSE_TEST_PG_PORT", "5433"))
    user = os.environ.get("SYNAPSE_POSTGRES_USER", "postgres")
    admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE IF EXISTS hamt_bench")
    admin.cursor().execute("CREATE DATABASE hamt_bench")
    admin.close()

    conn = psycopg2.connect(host=host, port=port, user=user, dbname="hamt_bench")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE state_hamt_nodes ("
        "structural_hash BYTEA PRIMARY KEY, node_bytes BYTEA NOT NULL)"
    )

    rng = random.Random(0)
    seen = 0
    for target in CUMULATIVE_SIZES:
        to_add = target - seen
        rows = rand_rows(rng, to_add)
        start = time.perf_counter()
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO state_hamt_nodes (structural_hash, node_bytes) VALUES %s",
            [(psycopg2.Binary(h), psycopg2.Binary(v)) for h, v in rows],
            page_size=1000,
        )
        elapsed = time.perf_counter() - start
        seen = target
        print(
            f"postgres   bulk-load +{to_add:>9,} rows in {elapsed:6.2f}s "
            f"({to_add / elapsed:,.0f} rows/s)"
        )

        cur.execute(
            "SELECT structural_hash FROM state_hamt_nodes TABLESAMPLE SYSTEM (1) LIMIT 5000"
        )
        keys_pool = [bytes(row[0]) for row in cur.fetchall()]

        def batch_fetch(keys: list[bytes]) -> None:
            cur.execute(
                "SELECT structural_hash, node_bytes FROM state_hamt_nodes "
                "WHERE structural_hash = ANY(%s)",
                ([psycopg2.Binary(k) for k in keys],),
            )
            cur.fetchall()

        def commit_write(rows: list[tuple[bytes, bytes]]) -> None:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO state_hamt_nodes (structural_hash, node_bytes) VALUES %s",
                [(psycopg2.Binary(h), psycopg2.Binary(v)) for h, v in rows],
            )

        bench_reads("postgres", target, batch_fetch, keys_pool)
        bench_commits("postgres", target, commit_write, rng)

    cur.close()
    conn.close()
    admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE IF EXISTS hamt_bench")
    admin.close()


def run_fjall() -> None:
    tmpdir = tempfile.mkdtemp(prefix="hamt-fjall-bench-")
    try:
        fjall_engine.open_client(tmpdir)

        rng = random.Random(0)
        seen = 0
        for target in CUMULATIVE_SIZES:
            to_add = target - seen
            rows = rand_rows(rng, to_add)
            start = time.perf_counter()
            fjall_engine.batch_put(rows)
            elapsed = time.perf_counter() - start
            seen = target
            print(
                f"fjall      bulk-load +{to_add:>9,} rows in {elapsed:6.2f}s "
                f"({to_add / elapsed:,.0f} rows/s)"
            )

            keys_pool = [h for h, _ in rows[:5000]]

            def batch_fetch(keys: list[bytes]) -> None:
                fjall_engine.batch_get(keys)

            def commit_write(rows: list[tuple[bytes, bytes]]) -> None:
                fjall_engine.transactional_batch_put(rows)

            bench_reads("fjall", target, batch_fetch, keys_pool)
            bench_commits("fjall", target, commit_write, rng)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> None:
    print(f"cumulative sizes: {CUMULATIVE_SIZES}, {NODE_SIZE}B nodes\n")
    print("--- fjall ---")
    run_fjall()
    print("\n--- postgres ---")
    run_postgres()


if __name__ == "__main__":
    main()
