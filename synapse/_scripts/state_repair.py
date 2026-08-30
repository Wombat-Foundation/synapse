#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#

import argparse
import json
import logging
from typing import Any, cast

import yaml

from twisted.internet import defer, reactor as reactor_

from synapse.config.homeserver import HomeServerConfig
from synapse.server import HomeServer
from synapse.storage import DataStore
from synapse.storage.database import LoggingTransaction
from synapse.types import ISynapseReactor

reactor = cast(ISynapseReactor, reactor_)
logger = logging.getLogger("synapse_state_repair")


class RepairHomeserver(HomeServer):
    DATASTORE_CLASS = DataStore

    def __init__(self, config: HomeServerConfig):
        super().__init__(
            hostname=config.server.server_name,
            config=config,
            reactor=reactor,
        )


def _load_room_ids(room: list[str], room_file: str | None) -> list[str]:
    room_ids = list(room)
    if room_file:
        with open(room_file) as f:
            room_ids.extend(
                line.strip()
                for line in f
                if line.strip() and not line.lstrip().startswith("#")
            )

    return sorted(set(room_ids))


def _count_from_row(row: tuple[Any, ...] | None) -> int:
    if row is None:
        return 0

    return int(row[0])


def _discover_room_txn(txn: LoggingTransaction, room_id: str) -> dict[str, Any]:
    txn.execute("SELECT room_version FROM rooms WHERE room_id = ?", (room_id,))
    room_row = txn.fetchone()

    txn.execute("SELECT 1 FROM partial_state_rooms WHERE room_id = ?", (room_id,))
    partial_state = txn.fetchone() is not None

    txn.execute(
        "SELECT event_id FROM event_forward_extremities WHERE room_id = ?",
        (room_id,),
    )
    forward_extremities = sorted(row[0] for row in txn.fetchall())

    txn.execute(
        "SELECT COUNT(*) FROM event_edges WHERE room_id = ? AND is_state",
        (room_id,),
    )
    state_edge_count = _count_from_row(txn.fetchone())

    txn.execute(
        "SELECT COUNT(*) FROM state_events WHERE room_id = ?",
        (room_id,),
    )
    state_event_count = _count_from_row(txn.fetchone())

    return {
        "room_id": room_id,
        "exists": room_row is not None,
        "room_version": room_row[0] if room_row else None,
        "partial_state": partial_state,
        "eligible_for_dag_replay": room_row is not None and not partial_state,
        "forward_extremities": forward_extremities,
        "state_edge_count": state_edge_count,
        "state_event_count": state_event_count,
    }


async def _run_discovery(hs: HomeServer, room_ids: list[str]) -> dict[str, Any]:
    store = hs.get_datastores().main

    rooms = []
    for room_id in room_ids:
        rooms.append(
            await store.db_pool.runInteraction(
                "synapse_state_repair_discover_room",
                _discover_room_txn,
                room_id,
            )
        )

    return {
        "mode": "dry_run",
        "rooms": rooms,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect rooms for future in-place state repair from the persisted DAG."
    )
    parser.add_argument("-v", action="store_true")
    parser.add_argument(
        "--config",
        type=argparse.FileType("r"),
        required=True,
        help="Synapse homeserver config.",
    )
    parser.add_argument(
        "--room",
        action="append",
        default=[],
        help="Room ID to inspect. May be repeated.",
    )
    parser.add_argument(
        "--room-file",
        help="File containing room IDs to inspect, one per line.",
    )
    parser.add_argument(
        "--write-report",
        help="Write JSON discovery report to this path instead of stdout.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Reserved for the future write-capable repair mode.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.v else logging.INFO,
        format="%(asctime)s - %(name)s - %(lineno)d - %(levelname)s - %(message)s",
    )

    if args.publish:
        parser.error(
            "--publish is not implemented; this command is currently read-only"
        )

    room_ids = _load_room_ids(args.room, args.room_file)
    if not room_ids:
        parser.error("at least one --room or --room-file entry is required")

    hs_config = yaml.safe_load(args.config)
    config = HomeServerConfig()
    config.parse_config_dict(hs_config, "", "")

    hs = RepairHomeserver(config)
    hs.setup()

    async def run() -> None:
        try:
            report = await _run_discovery(hs, room_ids)
            report_json = json.dumps(report, indent=2, sort_keys=True)
            if args.write_report:
                with open(args.write_report, "w") as f:
                    f.write(report_json)
                    f.write("\n")
            else:
                print(report_json)
        finally:
            reactor.stop()

    hs.get_clock().call_when_running(
        lambda: defer.ensureDeferred(
            hs.run_as_background_process("synapse_state_repair", run)
        )
    )
    reactor.run()


if __name__ == "__main__":
    main()
