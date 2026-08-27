#!/usr/bin/env bash
#
# Starts a single-node PD + TiKV cluster in Docker and waits for it to
# bootstrap. Used by complement_tests.yml and tests.yml (trial-tikv) and the
# sytest-tikv job -- kept as one script so they don't drift.
#
# Two modes:
#   - On the host (trial-tikv / complement-tikv): reachable at
#     172.17.0.1:2379 (PD) / 172.17.0.1:20160 (TiKV), the default bridge.
#   - Inside a container with the docker socket mounted (sytest-tikv): TiKV is
#     run on the SAME docker network as the current container, so the two can
#     reach each other by name (`pd`, `tikv`) without relying on the host's
#     bridge IP. Synapse connects to `pd:2379`.
#
set -eux

NETWORK=""
ADVERTISE_PD="172.17.0.1"
ADVERTISE_TIKV="172.17.0.1"

# If we're inside a container that has the docker socket mounted (sytest),
# detect our own network so PD/TiKV can run on it and be reached by name.
if [ -S /var/run/docker.sock ] && [ -n "$HOSTNAME" ] && docker inspect "$HOSTNAME" >/dev/null 2>&1; then
	NETWORK=$(docker inspect "$HOSTNAME" --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}' | awk '{print $1}')
	if [ -n "$NETWORK" ]; then
		ADVERTISE_PD="pd"
		ADVERTISE_TIKV="tikv"
		echo "Running TiKV on container network '$NETWORK' (reachable by name)"
	fi
fi

NETWORK_ARGS=()
if [ -n "$NETWORK" ]; then
	NETWORK_ARGS=(--network "$NETWORK")
fi

docker run -d --name pd "${NETWORK_ARGS[@]}" --ulimit nofile=1048576:1048576 -p 2379:2379 pingcap/pd:latest \
	--name=pd \
	--client-urls=http://0.0.0.0:2379 \
	--advertise-client-urls=http://$ADVERTISE_PD:2379 \
	--data-dir=/data

# Wait until PD has elected a leader. Some endpoints can respond
# before the single-node PD cluster is ready to bootstrap TiKV.
leader_found=0
for i in $(seq 1 30); do
	if curl --max-time 5 -fsS http://$ADVERTISE_PD:2379/pd/api/v1/leader; then
		leader_found=1
		break
	fi
	echo "Waiting for PD leader..."
	sleep 2
done

if [ "$leader_found" -ne 1 ]; then
	echo "PD did not elect a leader in time"
	docker logs pd || true
	exit 1
fi

# shellcheck disable=SC2086 # NETWORK_ARGS is intentionally two words (--network <name>)
docker run -d --name tikv "${NETWORK_ARGS[@]}" --ulimit nofile=1048576:1048576 -p 20160:20160 pingcap/tikv:latest \
	--addr=0.0.0.0:20160 \
	--advertise-addr=$ADVERTISE_TIKV:20160 \
	--data-dir=/data \
	--pd=$ADVERTISE_PD:2379

get_pd_count() {
	curl -fsS "http://$ADVERTISE_PD:2379/pd/api/v1/$1" 2>/dev/null |
		python3 -c 'import json, sys; print(json.load(sys.stdin).get("count", 0))' 2>/dev/null ||
		echo 0
}

for i in $(seq 1 60); do
	if [ "$(get_pd_count stores)" -ge 1 ] && [ "$(get_pd_count regions)" -ge 1 ]; then
		exit 0
	fi

	echo "Waiting for TiKV cluster bootstrap... ($i/60)"
	sleep 2
done

echo "TiKV cluster did not bootstrap in time"
echo "PD stores:"
curl -fsS http://$ADVERTISE_PD:2379/pd/api/v1/stores || true
echo "PD regions:"
curl -fsS http://$ADVERTISE_PD:2379/pd/api/v1/regions || true
docker logs pd || true
docker logs tikv || true
exit 1
