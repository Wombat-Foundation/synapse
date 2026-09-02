#!/usr/bin/env python
"""Focused follow-up to benchmark_hamt_storage_engines.py: read latency at
the batch sizes a *selective* (conflicted-set-only) HAMT lookup actually
uses -- 1, 5, 10 keys -- rather than the ~100-key BFS-materialize batch the
main benchmark covers. Both matter: Matrix state-res v2 separates a large
unconflicted state map (served from cache / full materialize) from a small
conflicted key set that's looked up selectively (see StateFilter usage in
synapse/storage/databases/state/store.py). Single fixed corpus size, since
the main benchmark already established both engines are flat across N.

Usage:
    eval "$(scripts-dev/start_test_postgres.sh)"
    python3 scripts-dev/benchmark_hamt_small_batches.py
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
CORPUS_SIZE = 2_000_000
BATCH_SIZES = (1, 5, 10)
ITERATIONS = 300


def rand_rows(rng: random.Random, n: int) -> list[tuple[bytes, bytes]]:
    return [(rng.randbytes(32), rng.randbytes(NODE_SIZE)) for _ in range(n)]


def percentiles(samples: list[float]) -> tuple[float, float]:
    samples = sorted(samples)
    p50 = statistics.median(samples) * 1e6
    p99 = samples[int(len(samples) * 0.99)] * 1e6
    return p50, p99


def bench(
    name: str,
    batch_size: int,
    batch_fetch: "Callable[[list[bytes]], object]",
    keys_pool: list[bytes],
) -> None:
    rng = random.Random(1)
    samples = []
    for _ in range(ITERATIONS):
        batch = rng.sample(keys_pool, batch_size)
        start = time.perf_counter()
        batch_fetch(batch)
        samples.append(time.perf_counter() - start)
    p50, p99 = percentiles(samples)
    print(f"{name:<10} batch={batch_size:<3} p50={p50:8.1f}us  p99={p99:8.1f}us")


def run_fjall() -> None:
    tmpdir = tempfile.mkdtemp(prefix="hamt-fjall-small-")
    try:
        fjall_engine.open_client(tmpdir)
        rng = random.Random(0)
        rows = rand_rows(rng, CORPUS_SIZE)
        fjall_engine.batch_put(rows)
        keys_pool = [h for h, _ in rows[:20000]]

        def batch_fetch(keys: list[bytes]) -> None:
            fjall_engine.batch_get(keys)

        for batch_size in BATCH_SIZES:
            bench("fjall", batch_size, batch_fetch, keys_pool)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def run_postgres() -> None:
    host = os.environ.get("SYNAPSE_POSTGRES_HOST", "/tmp/synapse-pgtest")
    port = int(os.environ.get("SYNAPSE_TEST_PG_PORT", "5433"))
    user = os.environ.get("SYNAPSE_POSTGRES_USER", "postgres")
    admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE IF EXISTS hamt_small_bench")
    admin.cursor().execute("CREATE DATABASE hamt_small_bench")
    admin.close()

    conn = psycopg2.connect(host=host, port=port, user=user, dbname="hamt_small_bench")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE state_hamt_nodes ("
        "structural_hash BYTEA PRIMARY KEY, node_bytes BYTEA NOT NULL)"
    )
    rng = random.Random(0)
    rows = rand_rows(rng, CORPUS_SIZE)
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO state_hamt_nodes (structural_hash, node_bytes) VALUES %s",
        [(psycopg2.Binary(h), psycopg2.Binary(v)) for h, v in rows],
        page_size=1000,
    )
    keys_pool = [h for h, _ in rows[:20000]]

    def batch_fetch(keys: list[bytes]) -> None:
        cur.execute(
            "SELECT structural_hash, node_bytes FROM state_hamt_nodes "
            "WHERE structural_hash = ANY(%s)",
            ([psycopg2.Binary(k) for k in keys],),
        )
        cur.fetchall()

    for batch_size in BATCH_SIZES:
        bench("postgres", batch_size, batch_fetch, keys_pool)

    cur.close()
    conn.close()
    admin = psycopg2.connect(host=host, port=port, user=user, dbname="postgres")
    admin.autocommit = True
    admin.cursor().execute("DROP DATABASE IF EXISTS hamt_small_bench")
    admin.close()


def run_mdbx() -> None:
    try:
        import mdbx  # type: ignore[import-untyped]
    except ImportError:
        print("mdbx not installed; skipping mdbx benchmark")
        return

    tmpdir = tempfile.mkdtemp(prefix="hamt-mdbx-small-")
    try:
        env = mdbx.Env(
            tmpdir, geometry=mdbx.Geometry(size_upper=10 * 1024 * 1024 * 1024)
        )
        rng = random.Random(0)
        rows = rand_rows(rng, CORPUS_SIZE)

        # Write rows in batch
        txn = env.rw_transaction()
        m = txn.open_map(None)
        for h, v in rows:
            m.put(txn, h, v)
        txn.commit()

        keys_pool = [h for h, _ in rows[:20000]]

        def batch_fetch(keys: list[bytes]) -> None:
            txn = env.ro_transaction()
            m = txn.open_map(None)
            for k in keys:
                m.get(txn, k)
            txn.abort()

        for batch_size in BATCH_SIZES:
            bench("mdbx", batch_size, batch_fetch, keys_pool)

        env.close()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main() -> None:
    print(
        f"corpus: {CORPUS_SIZE:,} nodes x {NODE_SIZE}B, {ITERATIONS} iterations/case\n"
    )
    run_fjall()
    print()
    run_mdbx()
    print()
    run_postgres()


if __name__ == "__main__":
    main()
