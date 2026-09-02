#
# Ad-hoc profiling script for event_auth_chain_links, run via trial against
# real Postgres so it goes through Synapse's real chain-cover persistence
# (not a synthetic table fill), then reports pg_stat_user_tables/indexes and
# EXPLAIN ANALYZE for the actual recursive-CTE query used by
# get_auth_chain_ids -- see synapse/storage/databases/main/event_federation.py
# _get_chain_links / _get_auth_chain_ids_using_cover_index_txn.
#
# Usage:
#   eval "$(scripts-dev/start_test_postgres.sh)"
#   SYNAPSE_POSTGRES=1 uv run trial tests.storage.test_profile_auth_chain_links
#
# Not a real regression test -- prints profiling output and is meant to be
# deleted after use.

import logging

from twisted.internet.testing import MemoryReactor

from synapse.rest import admin
from synapse.rest.client import login, room
from synapse.server import HomeServer
from synapse.util.clock import Clock

from tests import unittest

logger = logging.getLogger(__name__)

NUM_ROOMS = 150
USERS_PER_ROOM = 8
MESSAGES_PER_ROOM = 15


class ProfileAuthChainLinksTestCase(unittest.HomeserverTestCase):
    servlets = [
        admin.register_servlets,
        room.register_servlets,
        login.register_servlets,
    ]

    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.store = hs.get_datastores().main

    def test_profile(self) -> None:
        # A pool of users shared across rooms (registering thousands of
        # distinct users is unnecessarily slow and not what drives chain
        # count -- distinct room membership/power-level graphs do).
        user_toks = []
        for i in range(USERS_PER_ROOM * 3):
            user = self.register_user(f"chain_user_{i}", "pass")
            tok = self.login(f"chain_user_{i}", "pass")
            user_toks.append((user, tok))

        room_ids = []
        last_event_id = None
        for r in range(NUM_ROOMS):
            creator, creator_tok = user_toks[r % len(user_toks)]
            room_id = self.helper.create_room_as(creator, tok=creator_tok)
            room_ids.append(room_id)

            members = [
                user_toks[(r + j) % len(user_toks)] for j in range(USERS_PER_ROOM)
            ]
            for user, tok in members:
                if user != creator:
                    self.helper.join(room_id, user, tok=tok)

            for m in range(MESSAGES_PER_ROOM):
                user, tok = members[m % len(members)]
                last_event_id = self.helper.send(room_id, f"msg {m}", tok=tok)[
                    "event_id"
                ]

        assert last_event_id is not None
        room_id = room_ids[-1]

        # --- pg_stat_user_tables / pg_stat_user_indexes ---
        def dump_stats(txn) -> None:  # type: ignore[no-untyped-def]
            txn.execute(
                """
                SELECT relname, n_live_tup, seq_scan, seq_tup_read,
                       idx_scan, idx_tup_fetch, n_tup_ins, n_tup_upd, n_tup_del
                FROM pg_stat_user_tables
                WHERE relname IN
                    ('event_auth_chain_links', 'event_auth_chains',
                     'event_auth', 'events', 'event_json')
                ORDER BY relname
                """
            )
            print("\n=== pg_stat_user_tables ===")
            print(
                f"{'table':<26}{'live_tup':>10}{'seq_scan':>10}{'seq_tup_read':>14}"
                f"{'idx_scan':>10}{'idx_tup_fetch':>14}{'ins':>8}{'upd':>8}{'del':>8}"
            )
            for row in txn.fetchall():
                print(
                    f"{row[0]:<26}{row[1]:>10}{row[2]:>10}{row[3]:>14}"
                    f"{row[4]:>10}{row[5]:>14}{row[6]:>8}{row[7]:>8}{row[8]:>8}"
                )

            txn.execute(
                """
                SELECT relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch
                FROM pg_stat_user_indexes
                WHERE relname IN ('event_auth_chain_links', 'event_auth_chains')
                ORDER BY relname, indexrelname
                """
            )
            print("\n=== pg_stat_user_indexes ===")
            for row in txn.fetchall():
                print(
                    f"{row[0]:<26}{row[1]:<40}scans={row[2]:<8}"
                    f"tup_read={row[3]:<10}tup_fetch={row[4]}"
                )

            txn.execute(
                "SELECT pg_size_pretty(pg_total_relation_size('event_auth_chain_links')), "
                "pg_size_pretty(pg_total_relation_size('event_auth_chains')), "
                "pg_size_pretty(pg_total_relation_size('event_auth'))"
            )
            sizes = txn.fetchone()
            print(
                f"\nevent_auth_chain_links size={sizes[0]}  "
                f"event_auth_chains size={sizes[1]}  event_auth size={sizes[2]}"
            )

        self.get_success(self.store.db_pool.runInteraction("dump_stats", dump_stats))

        # --- EXPLAIN ANALYZE of the real query, using the room's current
        # forward-extremity state as a representative event_ids set (the
        # actual call shape used by state resolution / federation auth). ---
        storage_controllers = self.hs.get_storage_controllers()
        state_ids = self.get_success(
            storage_controllers.state.get_current_state_ids(room_id)
        )
        rep_event_ids = list(state_ids.values())

        def explain_cover_index(txn) -> None:  # type: ignore[no-untyped-def]
            from synapse.storage.database import make_in_list_sql_clause

            # Resolve chain ids for the representative event set, same as
            # _get_auth_chain_ids_using_cover_index_txn's first step.
            clause, args = make_in_list_sql_clause(
                txn.database_engine, "event_id", rep_event_ids
            )
            txn.execute(
                f"SELECT chain_id, sequence_number FROM event_auth_chains WHERE {clause}",
                args,
            )
            chain_rows = txn.fetchall()
            chains_to_fetch = {c for c, _ in chain_rows}
            print(
                f"\nrepresentative event set size={len(rep_event_ids)}, "
                f"resolved chains={len(chains_to_fetch)}"
            )

            if not chains_to_fetch:
                print("(no chains resolved -- room may not have a chain cover index)")
                return

            clause2, args2 = make_in_list_sql_clause(
                txn.database_engine, "origin_chain_id", tuple(chains_to_fetch)
            )
            sql = f"""
                EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
                WITH RECURSIVE links(chain_id) AS (
                    SELECT
                        DISTINCT origin_chain_id
                    FROM event_auth_chain_links WHERE {clause2}
                    UNION
                    SELECT
                        target_chain_id
                    FROM event_auth_chain_links
                    INNER JOIN links ON (chain_id = origin_chain_id)
                )
                SELECT
                    origin_chain_id, origin_sequence_number,
                    target_chain_id, target_sequence_number
                FROM links
                INNER JOIN event_auth_chain_links ON (chain_id = origin_chain_id)
            """
            txn.execute(sql, args2)
            print("\n=== EXPLAIN ANALYZE: _get_chain_links recursive CTE ===")
            for (line,) in txn.fetchall():
                print(line)

        self.get_success(
            self.store.db_pool.runInteraction(
                "explain_cover_index", explain_cover_index
            )
        )

        # --- Worst-case batch: _get_chain_links processes up to 1000 chain
        # ids per call (itertools.islice(chains_to_fetch, 1000)). Simulate
        # that batch size directly against every chain in the DB. ---
        def explain_full_batch(txn) -> None:  # type: ignore[no-untyped-def]
            from synapse.storage.database import make_in_list_sql_clause

            txn.execute("SELECT DISTINCT chain_id FROM event_auth_chains LIMIT 1000")
            all_chains = [c for (c,) in txn.fetchall()]
            print(
                f"\nfull-batch chains={len(all_chains)} (cap 1000, matches real "
                f"_get_chain_links batching)"
            )

            clause, args = make_in_list_sql_clause(
                txn.database_engine, "origin_chain_id", tuple(all_chains)
            )
            sql = f"""
                EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
                WITH RECURSIVE links(chain_id) AS (
                    SELECT
                        DISTINCT origin_chain_id
                    FROM event_auth_chain_links WHERE {clause}
                    UNION
                    SELECT
                        target_chain_id
                    FROM event_auth_chain_links
                    INNER JOIN links ON (chain_id = origin_chain_id)
                )
                SELECT
                    origin_chain_id, origin_sequence_number,
                    target_chain_id, target_sequence_number
                FROM links
                INNER JOIN event_auth_chain_links ON (chain_id = origin_chain_id)
            """
            txn.execute(sql, args)
            print("\n=== EXPLAIN ANALYZE: full 1000-chain batch ===")
            for (line,) in txn.fetchall():
                print(line)

        self.get_success(
            self.store.db_pool.runInteraction("explain_full_batch", explain_full_batch)
        )
