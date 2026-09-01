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
#   - Raising max_connections for parallel workers (`trial --jobs=N`)
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
	if [ -d "$PGDATA" ] && ! pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
		rm -rf "$PGDATA"
	fi
	echo "# Initializing throwaway RAM-disk PostgreSQL cluster at $PGDATA..." >&2
	mkdir -p "$PGDATA"
	# -U postgres makes "postgres" the bootstrap superuser directly (matching
	# what tests/server.py connects as) -- no separate `createuser` needed.
	# -E UTF8 --locale=C (not the combined "C.UTF-8", which isn't guaranteed
	# to exist on every system) matches the LC_COLLATE='C' LC_CTYPE='C'
	# tests/utils.py itself uses for POSTGRES_BASE_DB.
	initdb -D "$PGDATA" -U postgres --auth=trust -E UTF8 --locale=C >/dev/null

	cat >>"$PGDATA/postgresql.conf" <<EOF

# --- scripts-dev/start_test_postgres.sh: throwaway test cluster config ---
fsync = off
synchronous_commit = off
full_page_writes = off
# Headroom for `trial --jobs=N`: each worker's homeserver pool defaults to
# cp_max=5, plus setup/teardown connections outside the pool.
max_connections = 200
EOF

	pg_ctl -D "$PGDATA" \
		-o "-p $PGPORT -k $PGSOCKETDIR" \
		-l "$LOGFILE" start >/dev/null

	# No need to pre-create a template database here: tests/utils.py creates
	# and fully schema-migrates its own base database automatically on the
	# first test run (POSTGRES_BASE_DB, named after the trial process's PID),
	# then uses that as the WITH TEMPLATE source for every per-test database.
fi

cat <<EOF
export SYNAPSE_POSTGRES=1
export SYNAPSE_POSTGRES_USER=postgres
export SYNAPSE_POSTGRES_HOST=$PGSOCKETDIR
export SYNAPSE_POSTGRES_PORT=$PGPORT
EOF
