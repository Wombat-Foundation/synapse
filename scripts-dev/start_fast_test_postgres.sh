#!/usr/bin/env bash
set -euo pipefail

PG_DIR="/dev/shm/synapse-test-pg"
PG_PORT="5433"
PG_SOCKET_DIR="/tmp/synapse-pgtest"

echo "==> Setting up RAM-disk PostgreSQL cluster at $PG_DIR..."
if [ -d "$PG_DIR" ]; then
	pg_ctl -D "$PG_DIR" stop -m immediate >/dev/null 2>&1 || true
	rm -rf "$PG_DIR"
fi
rm -rf "$PG_SOCKET_DIR"
mkdir -p "$PG_DIR" "$PG_SOCKET_DIR"

initdb --locale=C.UTF-8 -D "$PG_DIR" >/dev/null

pg_ctl -D "$PG_DIR" -o "-k $PG_SOCKET_DIR -p $PG_PORT -c fsync=off -c synchronous_commit=off -c full_page_writes=off -c max_connections=200" start

echo "==> Creating test postgres user & template DB..."
createuser -h "$PG_SOCKET_DIR" -p "$PG_PORT" -s postgres || true
createdb -h "$PG_SOCKET_DIR" -p "$PG_PORT" -O postgres synapse_test_template || true

echo "
============================================================
Fast Test PostgreSQL is running!

Export these variables in your shell before running trial:

  export SYNAPSE_POSTGRES=1
  export SYNAPSE_POSTGRES_USER=postgres
  export SYNAPSE_POSTGRES_HOST=$PG_SOCKET_DIR
  export SYNAPSE_POSTGRES_PORT=$PG_PORT

To stop the instance later:
  pg_ctl -D $PG_DIR stop
============================================================
"
