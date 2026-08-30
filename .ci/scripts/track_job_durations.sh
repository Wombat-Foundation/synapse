#!/usr/bin/env bash
# Append per-job wall-clock durations from the "Tests" workflow to a CSV,
# so we can spot regressions (e.g. hybrid TiKV modes getting slower) over
# time instead of noticing by hand months later.
#
# Usage:
#   .ci/scripts/track_job_durations.sh [branch] [csv_path]
#
# Defaults to the current branch and docs/development-gg/ci-job-durations.csv.
# Safe to re-run: it skips (run_id, job_name) pairs already recorded.

set -euo pipefail

branch=${1:-$(git rev-parse --abbrev-ref HEAD)}
csv_path=${2:-docs/development-gg/ci-job-durations.csv}
workflow=${WORKFLOW_NAME:-tests.yml}
limit=${RUN_LIMIT:-20}

mkdir -p "$(dirname "$csv_path")"
if [ ! -f "$csv_path" ]; then
	echo "branch,run_id,run_started_at,job_id,job_name,duration_seconds,conclusion" >"$csv_path"
fi

echo "Fetching last $limit '$workflow' runs on branch '$branch'..." >&2

run_ids=$(gh run list --workflow "$workflow" --branch "$branch" --limit "$limit" \
	--json databaseId -q '.[].databaseId')

for run_id in $run_ids; do
	run_started_at=$(gh run view "$run_id" --json createdAt -q '.createdAt')

	gh run view "$run_id" --json jobs -q '
		.jobs[]
		| select(
			.startedAt != null and .completedAt != null
			and .startedAt != "0001-01-01T00:00:00Z"
			and .completedAt != "0001-01-01T00:00:00Z"
		)
		| [
			.databaseId,
			.name,
			((.completedAt | fromdateiso8601) - (.startedAt | fromdateiso8601)),
			.conclusion
		]
		| @tsv
	' | while IFS=$'\t' read -r job_id job_name duration_seconds conclusion; do
		# Skip if this (run_id, job_id) is already recorded.
		if grep -qF ",${run_id},${run_started_at},${job_id}," "$csv_path" 2>/dev/null; then
			continue
		fi
		# job_name may contain commas (e.g. "trial (3.10, postgres, 14, all)");
		# quote it for CSV safety.
		printf '%s,%s,%s,%s,"%s",%s,%s\n' \
			"$branch" "$run_id" "$run_started_at" "$job_id" "$job_name" \
			"$duration_seconds" "$conclusion" >>"$csv_path"
	done
done

echo "Updated $csv_path" >&2
