#!/usr/bin/env bash
#
# Starts a throwaway PostgreSQL cluster tuned for Synapse's test suite
# (`SYNAPSE_POSTGRES=1 trial ...`), running on RAM disk with durability disabled.
#
# Every test method creates and drops its own database (see `tests/server.py`),
# incurring thousands of `CREATE DATABASE ... WITH TEMPLATE ...` round-trips.
# This script eliminates disk IOPS bottlenecks by:
#   - Placing PGDATA on tmpfs (/dev/shm), executing all DB operations in RAM
#   - Disabling fsync, synchronous_commit, and full_page_writes
#   - Pre-creating synapse_test_template and expanding max_connections=200 for parallel workers (`trial -jN`)
#
# Usage:
#   eval "$(scripts-dev/start_test_postgres.sh)"      # start (idempotent) and export env vars
#   scripts-dev/start_test_postgres.sh stop          # stop and wipe RAM disk
#   scripts-dev/start_test_postgres.sh status        # check running status
#

set -euo pipefail

PGDATA="${SYNAPSE_TEST_PG_DATA:-/dev/shm/synapse-test-postgres}"
PGPORT="${SYNAPSE_TEST_PG_PORT:-5433}"
PGSOCKETDIR="/tmp/synapse-pgtest"
LOGFILE="$PGDATA.log"

ACTION="${1:-start}"

case "$ACTION" in
stop)
	if [ -d "$PGDATA" ]; then
		pg_ctl -D "$PGDATA" stop -m fast >/dev/null 2>&1 || true
		rm -rf "$PGDATA" "$PGSOCKETDIR" "$LOGFILE"
		echo "Stopped and cleaned RAM-disk PostgreSQL at $PGDATA" >&2
	else
		echo "Nothing running at $PGDATA" >&2
	fi
	exit 0
	;;

status)
	if [ -d "$PGDATA" ] && pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
		echo "PostgreSQL running at $PGDATA (port $PGPORT, socket $PGSOCKETDIR)" >&2
		exit 0
	else
		echo "PostgreSQL not running at $PGDATA" >&2
		exit 1
	fi
	;;

start)
	;;

*)
	echo "Usage: $0 {start|stop|status}" >&2
	exit 1
	;;
esac

mkdir -p "$PGSOCKETDIR"

if pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
	echo "# PostgreSQL cluster already running at $PGDATA" >&2
else
	echo "# Initializing throwaway RAM-disk PostgreSQL cluster at $PGDATA..." >&2
	mkdir -p "$PGDATA"
	initdb -D "$PGDATA" -U postgres --auth=trust -E UTF8 --locale=C.UTF-8 >/dev/null

	cat >>"$PGDATA/postgresql.conf" <<EOF

# --- scripts-dev/start_test_postgres.sh: throwaway test cluster config ---
fsync = off
synchronous_commit = off
full_page_writes = off
max_connections = 200
EOF

	pg_ctl -D "$PGDATA" \
		-o "-p $PGPORT -k $PGSOCKETDIR" \
		-l "$LOGFILE" start >/dev/null

	# Pre-create test user and template DB
	createuser -h "$PGSOCKETDIR" -p "$PGPORT" -s postgres >/dev/null 2>&1 || true
	createdb -h "$PGSOCKETDIR" -p "$PGPORT" -O postgres synapse_test_template >/dev/null 2>&1 || true
fi

cat <<EOF
export SYNAPSE_POSTGRES=1
export SYNAPSE_POSTGRES_USER=postgres
export SYNAPSE_POSTGRES_HOST=$PGSOCKETDIR
export SYNAPSE_POSTGRES_PORT=$PGPORT
EOF
