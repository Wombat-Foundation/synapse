#!/usr/bin/env bash
#
# Wired in via complement's COMPLEMENT_POST_TEST_SCRIPT hook (see
# executePostScript/Destroy in complement's internal/docker/deployer.go):
# Complement runs this once per homeserver, per test, while that
# homeserver's container is *still up* -- specifically before it stops or
# force-removes it -- and passes exactly:
#   $1  container id
#   $2  test name
#   $3  "true"/"false" -- whether the test failed
#
# Two things happen here, both meant to catch state Complement would
# otherwise destroy before anyone can look at it:
#
#   1. Dump a few cheap Postgres stats (only meaningful for
#      Postgres-backed runs; a no-op, not an error, on SQLite ones) to
#      tests/complement/pg_stats.log so DB activity/locks/deadlocks can be
#      compared test-to-test rather than only inspected live.
#
#   2. On failure, `docker commit` the container to a locally-tagged image
#      before Complement's Destroy() force-removes it. Complement itself
#      has no "keep failed containers" option (Destroy always removes
#      regardless of the `failed` flag), so this is the only hook point
#      that runs early enough to preserve anything. The saved image can
#      later be inspected with, e.g.:
#        docker run --rm -it --entrypoint sh <tag>
#        docker cp <a-container-from-that-image>:/data/embedded_hamt ./out
#
# Failures in here are deliberately non-fatal (Complement only logs
# executePostScript's error, it doesn't fail the test run on our account)
# but we still want them visible, so everything below goes to stderr/the
# stats log rather than being silently swallowed.

set -uo pipefail

container_id="${1:?missing container id}"
test_name="${2:?missing test name}"
failed="${3:-false}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runtime="${CONTAINER_RUNTIME:-docker}"

stats_dir="${repo_root}/.tmp/complement"
stats_log="${stats_dir}/pg_stats.log"
mkdir -p "$stats_dir"

{
	echo "=== $(date -u +%FT%TZ) test=${test_name} failed=${failed} container=${container_id} ==="
	if "$runtime" exec -u postgres "$container_id" pg_isready -q 2>/dev/null; then
		echo "--- pg_stat_database ---"
		"$runtime" exec -u postgres "$container_id" psql -X -q -At -c "
      SELECT datname, numbackends, xact_commit, xact_rollback, deadlocks, conflicts, blks_hit, blks_read
      FROM pg_stat_database
      WHERE datname NOT IN ('template0', 'template1');" 2>&1
		echo "--- pg_stat_activity (non-idle) ---"
		"$runtime" exec -u postgres "$container_id" psql -X -q -At -c "
      SELECT pid, state, wait_event_type, wait_event, now() - query_start AS running_for, left(query, 120)
      FROM pg_stat_activity
      WHERE state IS NOT NULL AND state != 'idle';" 2>&1
		echo "--- pg_locks (not granted) ---"
		"$runtime" exec -u postgres "$container_id" psql -X -q -At -c "
      SELECT pid, mode, locktype, relation::regclass, granted
      FROM pg_locks
      WHERE NOT granted;" 2>&1
	else
		echo "(postgres not up in this container -- SQLite run, or not ready yet; skipping)"
	fi
} >>"$stats_log" 2>&1

if [[ "$failed" == "true" ]]; then
	safe_name="$(printf '%s' "$test_name" | tr -c 'A-Za-z0-9_.-' '_')"
	tag="complement-failed-debug:${safe_name}-$(date +%s)"
	if "$runtime" commit "$container_id" "$tag" >/dev/null 2>&1; then
		echo "saved failing container ${container_id} (test ${test_name}) as image ${tag}" >&2
		echo "${tag}" >>"${stats_dir}/failed_images.txt"
	else
		echo "WARN: failed to ${runtime} commit ${container_id} for ${test_name}" >&2
	fi
fi

exit 0
