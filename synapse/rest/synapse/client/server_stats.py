# Copyright 2026 Synapse Authors
# Licensed under AGPLv3

from http import HTTPStatus
from typing import TYPE_CHECKING

from synapse.api.constants import Direction
from synapse.http.server import DirectServeJsonResource
from synapse.http.site import SynapseRequest
from synapse.storage.databases.main.room import RoomSortOrder
from synapse.storage.databases.main.transactions import DestinationSortOrder
from synapse.types import JsonDict

if TYPE_CHECKING:
    from synapse.server import HomeServer


class ServerStatsResource(DirectServeJsonResource):
    """Public server statistics endpoint for landing page and monitoring."""

    def __init__(self, hs: "HomeServer"):
        super().__init__(clock=hs.get_clock())
        self.hs = hs
        self.store = hs.get_datastores().main

    async def _async_on_GET(self, request: SynapseRequest) -> tuple[int, JsonDict]:
        total_users = await self.store.count_all_users()
        public_rooms = await self.store.count_public_rooms(
            network_tuple=None,
            ignore_non_federatable=False,
            search_filter=None,
        )
        try:
            _, total_rooms = await self.store.get_rooms_paginate(
                start=0,
                limit=1,
                order_by=RoomSortOrder.NAME.value,
                reverse_order=False,
                search_term=None,
                public_rooms=None,
                empty_rooms=None,
            )
        except Exception:
            total_rooms = public_rooms

        try:
            _, total_destinations = await self.store.get_destinations_paginate(
                start=0,
                limit=1,
                destination=None,
                order_by=DestinationSortOrder.DESTINATION.value,
                direction=Direction.FORWARDS,
            )
        except Exception:
            total_destinations = 0

        return HTTPStatus.OK, {
            "total_users": total_users,
            "total_rooms": total_rooms,
            "public_rooms": public_rooms,
            "total_destinations": total_destinations,
            "server_version": self.hs.version_string,
        }
