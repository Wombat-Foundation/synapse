#!/bin/sh
#
# Starts a single-node PD + TiKV cluster in Docker, reachable at
# 172.17.0.1:2379 (PD) / 172.17.0.1:20160 (TiKV), and waits for it to
# bootstrap. Used by both complement_tests.yml and tests.yml (trial-tikv) --
# kept as one script so the two don't drift.
#
set -eux

docker run -d --name pd --ulimit nofile=1048576:1048576 -p 2379:2379 pingcap/pd:latest \
  --name=pd \
  --client-urls=http://0.0.0.0:2379 \
  --advertise-client-urls=http://172.17.0.1:2379 \
  --data-dir=/data

# Wait until PD has elected a leader. Some endpoints can respond
# before the single-node PD cluster is ready to bootstrap TiKV.
until curl -fsS http://127.0.0.1:2379/pd/api/v1/leader; do
  echo "Waiting for PD leader..."
  sleep 2
done

docker run -d --name tikv --ulimit nofile=1048576:1048576 -p 20160:20160 pingcap/tikv:latest \
  --addr=0.0.0.0:20160 \
  --advertise-addr=172.17.0.1:20160 \
  --data-dir=/data \
  --pd=172.17.0.1:2379

get_pd_count() {
  curl -fsS "http://127.0.0.1:2379/pd/api/v1/$1" 2>/dev/null |
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
curl -fsS http://127.0.0.1:2379/pd/api/v1/stores || true
echo "PD regions:"
curl -fsS http://127.0.0.1:2379/pd/api/v1/regions || true
docker logs pd || true
docker logs tikv || true
exit 1
