#!/usr/bin/env bash
#
# Starts a throwaway PostgreSQL cluster tuned for running Synapse's test
# suite (`SYNAPSE_POSTGRES=1 trial ...`), not for anything resembling
# production use.
#
# Every test method creates and drops its own database (see
# `setup_test_homeserver` in tests/server.py), so a full trial run pays for
# thousands of `CREATE DATABASE ... WITH TEMPLATE ...` / `DROP DATABASE`
# round-trips. None of that data needs to survive a crash or a reboot, so
# this script:
#
#   - puts PGDATA on tmpfs (/dev/shm), avoiding real disk I/O entirely
#   - disables fsync, synchronous_commit and full_page_writes, since there's
#     nothing here worth protecting against a crash
#
# Usage:
#   scripts-dev/start_test_postgres.sh          # start (idempotent)
#   scripts-dev/start_test_postgres.sh stop     # stop and wipe
#
# On success, prints the environment variables to export before running
# trial, e.g.:
#   eval "$(scripts-dev/start_test_postgres.sh)"
#   uv run trial tests

set -euo pipefail

PGDATA="${SYNAPSE_TEST_PG_DATA:-/dev/shm/synapse-test-postgres}"
PGPORT="${SYNAPSE_TEST_PG_PORT:-5433}"
PGSOCKETDIR="$PGDATA"
LOGFILE="$PGDATA.log"

if [ "${1:-}" = "stop" ]; then
	if [ -d "$PGDATA" ]; then
		pg_ctl -D "$PGDATA" stop -m fast >/dev/null 2>&1 || true
		rm -rf "$PGDATA" "$LOGFILE"
		echo "Stopped and removed $PGDATA" >&2
	else
		echo "Nothing running at $PGDATA" >&2
	fi
	exit 0
fi

if pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
	echo "Already running at $PGDATA" >&2
else
	mkdir -p "$PGDATA"
	initdb -D "$PGDATA" -U postgres --auth=trust -E UTF8 --locale=C >/dev/null

	# Belt-and-braces on top of the initdb defaults: these are the settings
	# that actually matter for test speed, applied whether or not initdb's
	# own defaults change out from under us.
	cat >>"$PGDATA/postgresql.conf" <<EOF

# --- scripts-dev/start_test_postgres.sh: throwaway test cluster, no durability needed ---
fsync = off
synchronous_commit = off
full_page_writes = off
EOF

	pg_ctl -D "$PGDATA" \
		-o "-p $PGPORT -k $PGSOCKETDIR -c listen_addresses=''" \
		-l "$LOGFILE" start >/dev/null
fi

cat <<EOF
export SYNAPSE_POSTGRES=1
export SYNAPSE_POSTGRES_USER=postgres
export SYNAPSE_POSTGRES_HOST=$PGSOCKETDIR
export SYNAPSE_POSTGRES_PORT=$PGPORT
EOF
