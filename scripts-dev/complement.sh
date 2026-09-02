#!/usr/bin/env bash
# This script is designed for developers who want to test their code
# against Complement.
#
# It makes a Synapse image which represents the current checkout,
# builds a synapse-complement image on top, then runs tests with it.
#
# By default the script will fetch the latest Complement main branch and
# run tests with that. This can be overridden to use a custom Complement
# checkout by setting the COMPLEMENT_DIR environment variable to the
# filepath of a local Complement checkout or by setting the COMPLEMENT_REF
# environment variable to pull a different branch or commit.
#
# To use the 'podman' command instead 'docker', set the PODMAN environment
# variable. Example:
#
# PODMAN=1 ./complement.sh
#
# By default Synapse is run in monolith mode. This can be overridden by
# setting the WORKERS environment variable.
#
# You can optionally give a "-f" argument (for "fast") before any to skip
# rebuilding the docker images, if you just want to rerun the tests.
#
# Remaining commandline arguments are passed through to `go test`. For example,
# you can supply a regular expression of test method names via the "-run"
# argument:
#
# ./complement.sh -run "TestOutboundFederation(Profile|Send)"
#
# Specifying TEST_ONLY_SKIP_DEP_HASH_VERIFICATION=1 will cause `poetry export`
# to not emit any hashes when building the Docker image. This then means that
# you can use 'unverifiable' sources such as git repositories as dependencies.

# Exit if a line returns a non-zero exit code
set -e

# Tag local builds with a dummy registry namespace so that later builds may reference
# them exactly instead of accidentally pulling from a remote registry.
#
# This is important as some Docker storage drivers/types prefer remote images over local
# (like `containerd`) which causes problems as we're testing against some remote image
# that doesn't include all of the changes that we're trying to test (be it locally or in
# a PR in CI). This is spawning from a real-world problem where the GitHub runners were
# updated to use Docker Engine 29.0.0+ which uses `containerd` by default for new
# installations.
#
# XXX: If the Docker image name changes, don't forget to update
# `.github/workflows/push_complement_image.yml` as well
LOCAL_IMAGE_NAMESPACE=localhost

# The image tags for how these images will be stored in the registry
SYNAPSE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/synapse"
SYNAPSE_WORKERS_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/synapse-workers"
# XXX: If the Docker image name changes, don't forget to update
# `.github/workflows/push_complement_image.yml` as well
COMPLEMENT_SYNAPSE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/complement-synapse"

SYNAPSE_EDITABLE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/synapse-editable"
SYNAPSE_WORKERS_EDITABLE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/synapse-workers-editable"
COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH="$LOCAL_IMAGE_NAMESPACE/complement-synapse-editable"

# Helper to emit annotations that collapse portions of the log in GitHub Actions
echo_if_github() {
  if [[ -n "$GITHUB_WORKFLOW" ]]; then
    echo $*
  fi
}

# Helper to print out the usage instructions
usage() {
    cat >&2 <<EOF
Usage: $0 [-f] <go test arguments>...
Run the complement test suite on Synapse.
  --in-repo
        Whether to run the in-repo suite of Complement tests (see `./complement` in this project)
        vs the Complement tests from the Complement repo.

  -f, --fast
        Skip rebuilding the docker images, and just use the most recent
        'localhost/complement-synapse:latest' image.
        Conflicts with --build-only.

  --build-only
        Only build the Docker images. Don't actually run Complement.
        Conflicts with -f/--fast.

  -e, --editable
        Use an editable build of Synapse, rebuilding the image if necessary.
        This is suitable for use in development where a fast turn-around time
        is important.
        Not suitable for use in CI in case the editable environment is impure.

  --rebuild-editable
        Force a rebuild of the editable build of Synapse.
        This is occasionally useful if the built-in rebuild detection with
        --editable fails, e.g. when changing configure_workers_and_start.py.

For help on arguments to 'go test', run 'go help testflag'.
EOF
}

# We use a function to wrap the script logic so that we can use `return` to exit early
# if needed. This is particularly useful so that this script can be sourced by other
# scripts without exiting the calling subshell (composable). This allows us to share
# variables like `SYNAPSE_SUPPORTED_COMPLEMENT_TEST_PACKAGES` with other scripts.
#
# Returns an exit code of 0 on success, or 1 on failure.
main() {
  # parse our arguments
  skip_docker_build=""
  skip_complement_run=""
  use_in_repo_tests=""
  while [ $# -ge 1 ]; do
    arg=$1
    case "$arg" in
      "-h")
        usage
        return 1
        ;;
      "--in-repo")
        use_in_repo_tests=1
        ;;
      "-f"|"--fast")
        skip_docker_build=1
        ;;
      "--build-only")
        skip_complement_run=1
        ;;
      "-e"|"--editable")
        use_editable_synapse=1
        ;;
      "--rebuild-editable")
        rebuild_editable_synapse=1
        ;;
      *)
        # unknown arg: presumably an argument to gotest. break the loop.
        break
    esac
    shift
  done

  # enable buildkit for the docker builds
  export DOCKER_BUILDKIT=1

  # Determine whether to use the docker or podman container runtime.
  if [ -n "$PODMAN" ]; then
    export CONTAINER_RUNTIME=podman
    export DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock
    export BUILDAH_FORMAT=docker
    export COMPLEMENT_HOSTNAME_RUNNING_COMPLEMENT=host.containers.internal
  else
    export CONTAINER_RUNTIME=docker
  fi

  # Change to the repository root
  cd "$(dirname $0)/.."

  # Check for a user-specified Complement checkout
  if [[ -z "$COMPLEMENT_DIR" ]]; then
    COMPLEMENT_REF=${COMPLEMENT_REF:-main}
    COMPLEMENT_REPO=${COMPLEMENT_REPO:-matrix-org/complement}
    echo "COMPLEMENT_DIR not set. Fetching ${COMPLEMENT_REPO} at ${COMPLEMENT_REF}..."

    # Download the Complement checkout at the specified ref.
    wget -q -O "${COMPLEMENT_REF}.tar.gz" "https://github.com/${COMPLEMENT_REPO}/archive/${COMPLEMENT_REF}.tar.gz"

    # Delete the existing complement checkout. Otherwise we'll end up with stale
    # test files after they're deleted server-side, and `tar` will not delete
    # old files.
    rm -rf complement-${COMPLEMENT_REF}

    # Extract the checkout.
    tar -xzf "${COMPLEMENT_REF}.tar.gz"

    COMPLEMENT_DIR=complement-${COMPLEMENT_REF}
    echo "Checkout available at 'complement-${COMPLEMENT_REF}'"
  fi

  if [[ -z "$use_in_repo_tests" ]] && [[ "$(realpath "$COMPLEMENT_DIR")" == "$(realpath ./complement)" ]]; then
    echo "COMPLEMENT_DIR points at this repository's in-repo Complement tests." >&2
    echo "Use --in-repo with COMPLEMENT_DIR=./complement, or unset COMPLEMENT_DIR to test against upstream Complement." >&2
    return 1
  fi

  # Compute this before deciding whether to rebuild images. The version-check
  # test also runs with --fast and --editable, where the standard-image build
  # branch below is skipped.
  pkg_version="$(sed -n 's/^version = "\(.*\)"$/\1/p' pyproject.toml | head -n1)"
  git_branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  if [ -n "$git_branch" ]; then git_branch="b=$git_branch"; fi
  git_tag="$(git describe --exact-match 2>/dev/null || true)"
  if [ -n "$git_tag" ]; then git_tag="t=$git_tag"; fi
  git_commit="$(git rev-parse --short HEAD 2>/dev/null || true)"
  git_dirty=""
  if git describe --dirty=-this_is_a_dirty_checkout 2>/dev/null | grep -q -- '-this_is_a_dirty_checkout$'; then
    git_dirty="dirty"
  fi
  git_version="$(IFS=,; echo "${git_branch:+$git_branch,}${git_tag:+$git_tag,}${git_commit:+$git_commit,}${git_dirty:+$git_dirty,}" | sed 's/,$//')"
  if [ -n "$git_version" ]; then
    synapse_version_string="$pkg_version ($git_version)"
  else
    synapse_version_string="$pkg_version"
  fi
  export SYNAPSE_VERSION_STRING="$synapse_version_string"

  if [ -n "$use_editable_synapse" ]; then
    if [[ -e synapse/synapse_rust.abi3.so ]]; then
      # In an editable install, back up the host's compiled Rust module to prevent
      # inconvenience; the container will overwrite the module with its own copy.
      mv -n synapse/synapse_rust.abi3.so synapse/synapse_rust.abi3.so~host
      # And restore it on exit:
      synapse_pkg=`realpath synapse`
      trap "mv -f '$synapse_pkg/synapse_rust.abi3.so~host' '$synapse_pkg/synapse_rust.abi3.so'" EXIT
    fi

    editable_mount="$(realpath .):/editable-src:z"
    if [ -n "$rebuild_editable_synapse" ]; then
      unset skip_docker_build
    elif $CONTAINER_RUNTIME inspect "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" &>/dev/null; then
      # complement-synapse-editable already exists: see if we can still use it:
      # - The Rust module must still be importable; it will fail to import if the Rust source has changed.
      # - The uv lock file must be the same (otherwise we assume dependencies have changed)

      # First set up the module in the right place for an editable installation.
      $CONTAINER_RUNTIME run --rm -v $editable_mount --entrypoint 'cp' "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" -- /synapse_rust.abi3.so.bak /editable-src/synapse/synapse_rust.abi3.so

      if ($CONTAINER_RUNTIME run --rm -v $editable_mount --entrypoint 'python' "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" -c 'import synapse.synapse_rust' \
        && $CONTAINER_RUNTIME run --rm -v $editable_mount --entrypoint 'diff' "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" --brief /editable-src/uv.lock /uv.lock.bak); then
        skip_docker_build=1
      else
        echo "Editable Synapse image is stale. Will rebuild."
        unset skip_docker_build
      fi
    fi
  fi

  if [ -z "$skip_docker_build" ]; then
    if [ -n "$use_editable_synapse" ]; then

      # Build a special image designed for use in development with editable
      # installs.
      $CONTAINER_RUNTIME build ${DOCKER_BUILD_ARGS:-} \
        -t "$SYNAPSE_EDITABLE_IMAGE_PATH" \
        -f "docker/editable.Dockerfile" .

      $CONTAINER_RUNTIME build ${DOCKER_BUILD_ARGS:-} \
        -t "$SYNAPSE_WORKERS_EDITABLE_IMAGE_PATH" \
        --build-arg FROM="$SYNAPSE_EDITABLE_IMAGE_PATH" \
        -f "docker/Dockerfile-workers" .

      $CONTAINER_RUNTIME build ${DOCKER_BUILD_ARGS:-} \
        -t "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" \
        --build-arg FROM="$SYNAPSE_WORKERS_EDITABLE_IMAGE_PATH" \
        -f "docker/complement/Dockerfile" "docker/complement"

      # Prepare the Rust module
      $CONTAINER_RUNTIME run --rm -v $editable_mount --entrypoint 'cp' "$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH" -- /synapse_rust.abi3.so.bak /editable-src/synapse/synapse_rust.abi3.so

    else
      # We remove the `egg-info` as it can contain outdated information which won't line
      # up with our current reality.
      rm -rf matrix_synapse.egg-info/
      # Build the base Synapse image from the local checkout
      echo_if_github "::group::Build Docker image: matrixdotorg/synapse"
      $CONTAINER_RUNTIME build ${DOCKER_BUILD_ARGS:-} \
        -t "$SYNAPSE_IMAGE_PATH" \
        --build-arg SYNAPSE_VERSION_STRING="$synapse_version_string" \
        --build-arg TEST_ONLY_SKIP_DEP_HASH_VERIFICATION \
        --build-arg TEST_ONLY_IGNORE_LOCKFILE \
        -f "docker/Dockerfile" .
      echo_if_github "::endgroup::"

      # Build the workers docker image (from the base Synapse image we just built).
      echo_if_github "::group::Build Docker image: matrixdotorg/synapse-workers"
      $CONTAINER_RUNTIME build ${DOCKER_BUILD_ARGS:-} \
        -t "$SYNAPSE_WORKERS_IMAGE_PATH" \
        --build-arg FROM="$SYNAPSE_IMAGE_PATH" \
        -f "docker/Dockerfile-workers" .
      echo_if_github "::endgroup::"

      # Build the unified Complement image (from the worker Synapse image we just built).
      echo_if_github "::group::Build Docker image: complement/Dockerfile"
      $CONTAINER_RUNTIME build ${DOCKER_BUILD_ARGS:-} \
        -t "$COMPLEMENT_SYNAPSE_IMAGE_PATH" \
        --build-arg FROM="$SYNAPSE_WORKERS_IMAGE_PATH" \
        -f "docker/complement/Dockerfile" "docker/complement"
      echo_if_github "::endgroup::"

    fi
  
    echo "Docker images built."
  else
    echo "Skipping Docker image build as requested."
  fi

  # Default set of Complement tests to run from the Complement repo
  #
  # We pick and choose the specific MSC's that Synapse supports.
  default_complement_test_packages=(
    ./tests/csapi
    ./tests
    ./tests/msc3874
    ./tests/msc3890
    ./tests/msc3391
    ./tests/msc3757
    ./tests/msc3930
    ./tests/msc3902
    ./tests/msc3967
    ./tests/msc4140
    ./tests/msc4155
    ./tests/msc4306
    ./tests/msc4222
    ./tests/msc4429
    ./tests/msc4499
  )

  available_complement_test_packages=()
  for test_package in "${default_complement_test_packages[@]}"; do
    if [[ -d "$COMPLEMENT_DIR/$test_package" ]]; then
      available_complement_test_packages+=("$test_package")
    else
      echo "Skipping unavailable Complement test package: $test_package" >&2
    fi
  done

  # Export the list of test packages as a space-separated environment variable, so other
  # scripts can use it.
  export SYNAPSE_SUPPORTED_COMPLEMENT_TEST_PACKAGES="${available_complement_test_packages[@]}"

  # Default set of Complement tests to run when using the in-repo test suite. Most
  # likely, this should be all tests.
  #
  # Relative to the `./complement` repo in this project
  default_in_repo_complement_test_packages=(
    ./tests/...
  )

  export COMPLEMENT_BASE_IMAGE="$COMPLEMENT_SYNAPSE_IMAGE_PATH"
  if [ -n "$use_editable_synapse" ]; then
    export COMPLEMENT_BASE_IMAGE="$COMPLEMENT_SYNAPSE_EDITABLE_IMAGE_PATH"
    export COMPLEMENT_HOST_MOUNTS="$editable_mount"
  fi

  # Enable dirty runs, so tests will reuse the same container where possible.
  # This significantly speeds up tests, but increases the possibility of test pollution.
  export COMPLEMENT_ENABLE_DIRTY_RUNS=1

  # All environment variables starting with PASS_ will be shared.
  # (The prefix is stripped off before reaching the container.)
  export COMPLEMENT_SHARE_ENV_PREFIX=PASS_

  # * -tags=synapse_blacklist: Enable the `synapse_blacklist` build tag, which is
  #   necessary for `runtime.Synapse` checks/skips to work in the tests
  test_tags="synapse_blacklist"

  # It takes longer than 10m to run the whole suite.
  test_timeout="60m"

  # Number of packages to run in parallel. Default 2 matches congruent's
  # COMPLEMENT_PARALLEL=2 — go test defaults to GOMAXPROCS which can spin up
  # enough containers simultaneously to cause 502s on registration.
  test_parallel="${COMPLEMENT_PARALLEL:-2}"

  if [[ -n "$WORKERS" ]]; then
    # Use workers.
    export PASS_SYNAPSE_COMPLEMENT_USE_WORKERS=true

    # Pass through the workers defined. If none, it will be an empty string
    export PASS_SYNAPSE_WORKER_TYPES="$WORKER_TYPES"

    # Workers can only use Postgres as a database.
    export PASS_SYNAPSE_COMPLEMENT_DATABASE=postgres

    # And provide some more configuration to complement.

    # It can take quite a while to spin up a worker-mode Synapse for the first
    # time (the main problem is that we start 14 python processes for each test,
    # and complement likes to do two of them in parallel).
    export COMPLEMENT_SPAWN_HS_TIMEOUT_SECS=120
  else
    export PASS_SYNAPSE_COMPLEMENT_USE_WORKERS=
    if [[ -n "$POSTGRES" ]]; then
      export PASS_SYNAPSE_COMPLEMENT_DATABASE=postgres
    else
      export PASS_SYNAPSE_COMPLEMENT_DATABASE=sqlite
    fi
  fi

  if [[ -n "$ASYNCIO_REACTOR" ]]; then
    # Enable the Twisted asyncio reactor
    export PASS_SYNAPSE_COMPLEMENT_USE_ASYNCIO_REACTOR=true
  fi

  if [[ -n "$UNIX_SOCKETS" ]]; then
    # Enable full on Unix socket mode for Synapse, Redis and Postgresql
    export PASS_SYNAPSE_USE_UNIX_SOCKET=1
  fi

  if [[ -n "$SYNAPSE_TEST_LOG_LEVEL" ]]; then
    # Set the log level to what is desired
    export PASS_SYNAPSE_LOG_LEVEL="$SYNAPSE_TEST_LOG_LEVEL"

    # Allow logging sensitive things (currently SQL queries & parameters).
    # (This won't have any effect if we're not logging at DEBUG level overall.)
    # Since this is just a test suite, this is fine and won't reveal anyone's
    # personal information
    export PASS_SYNAPSE_LOG_SENSITIVE=1
  fi

  # Log a few more useful things for a developer attempting to debug something
  # particularly tricky.
  export PASS_SYNAPSE_LOG_TESTING=1

  if [[ -n "$SYNAPSE_TIKV_PD_ENDPOINTS" ]]; then
    export PASS_SYNAPSE_TIKV_PD_ENDPOINTS="$SYNAPSE_TIKV_PD_ENDPOINTS"
  fi

  # ── Run-filter and extra-tags from remaining args ───────────────────────────
  # RUN_TESTS=. means "run everything" (the default).
  # -run PATTERN and -run=PATTERN are extracted for package narrowing + anchoring.
  # -tags TAG and -tags=TAG are merged into test_tags (never forwarded as a
  # second -tags flag which go test would silently clobber the first with).
  # Everything else goes into extra_args and is forwarded verbatim.
  RUN_TESTS="${COMPLEMENT_RUN:-.}"
  local -a extra_args=()
  local _i=1
  while [ $_i -le $# ]; do
    local _arg="${!_i}"
    if [[ "$_arg" == "-run" ]]; then
      local _next=$((_i+1))
      RUN_TESTS="${!_next}"
      _i=$((_i+2))
    elif [[ "$_arg" =~ ^-run=(.+) ]]; then
      RUN_TESTS="${BASH_REMATCH[1]}"
      _i=$((_i+1))
    elif [[ "$_arg" == "-tags" ]]; then
      local _next=$((_i+1))
      test_tags="${test_tags},${!_next}"
      _i=$((_i+2))
    elif [[ "$_arg" =~ ^-tags=(.+) ]]; then
      test_tags="${test_tags},${BASH_REMATCH[1]}"
      _i=$((_i+1))
    else
      extra_args+=("$_arg")
      _i=$((_i+1))
    fi
  done

  # ── Staged result / log files (timestamped, never overwrite) ────────────────
  local repo_root
  repo_root="$(realpath "$(dirname "$0")/..")"
  local results_dir="${RESULTS_DIR:-tests/complement}"
  local main_results_file="${repo_root}/${results_dir}/results.jsonl"
  local main_log_file="${repo_root}/${results_dir}/logs.jsonl"
  mkdir -p "$(dirname "$main_results_file")"
  touch "$main_results_file" "$main_log_file"

  local run_suffix
  if [ "$RUN_TESTS" = "." ]; then
    run_suffix="all"
  else
    run_suffix="$(echo "$RUN_TESTS" | sed 's/[^a-zA-Z0-9]/_/g' | cut -c1-32)"
    run_suffix="${run_suffix:-all}"
  fi
  local run_stamp
  run_stamp="$(date +%s%N)"
  local staging_dir="${repo_root}/.tmp/complement"
  mkdir -p "$staging_dir"
  local staged_log_file="${staging_dir}/logs.${run_suffix}.${run_stamp}.jsonl"
  local staged_results_file="${staging_dir}/test_results.${run_suffix}.${run_stamp}.jsonl"
  : >"$staged_log_file"
  : >"$staged_results_file"

  echo ""
  echo "running go test with:"
  echo "\$COMPLEMENT_DIR: ${COMPLEMENT_DIR:-<auto>}"
  echo "\$COMPLEMENT_BASE_IMAGE: $COMPLEMENT_BASE_IMAGE"
  echo "\$staged_results_file (staging): $staged_results_file"
  echo "\$main_results_file: $main_results_file"
  echo "\$staged_log_file: $staged_log_file"
  echo "\$RUN_TESTS: $RUN_TESTS"
  echo ""

  # ── anchor_one: per-segment ^ anchoring so -run TestFoo doesn't match TestFooBar ──
  anchor_one() {
    local pattern="$1"
    local -a anchored=()
    local -a segments
    IFS='/' read -r -a segments <<<"$pattern"
    local last=$(( ${#segments[@]} - 1 ))
    local idx=0
    for segment in "${segments[@]}"; do
      if [[ "$segment" =~ ^\^ || "$segment" =~ .*[][()?.+*|$] ]]; then
        anchored+=("$segment")
      elif [ "$idx" -eq "$last" ]; then
        anchored+=("^${segment}")
      else
        anchored+=("^${segment}\$")
      fi
      idx=$((idx+1))
    done
    (IFS='/'; echo "${anchored[*]}")
  }

  # Split top-level | into separate go test invocations (go test's -run re-splits
  # on every /, silently dropping one side of alternations with differing depth).
  local -a ALT_PATTERNS=()
  if [ "$RUN_TESTS" = "." ]; then
    ALT_PATTERNS=(".")
  else
    local -a raw_alts
    IFS='|' read -r -a raw_alts <<<"$RUN_TESTS"
    for alt in "${raw_alts[@]}"; do
      ALT_PATTERNS+=("$(anchor_one "$alt")")
    done
    if [ "${#ALT_PATTERNS[@]}" -gt 1 ]; then
      echo "Anchored run regexes (one go test invocation each):"
      for alt in "${ALT_PATTERNS[@]}"; do echo "  $alt"; done
    else
      echo "Anchored run regex: ${ALT_PATTERNS[0]}"
    fi
  fi

  # ── Container token + cleanup trap ──────────────────────────────────────────
  export COMPLEMENT_WRAPPER_TOKEN="${COMPLEMENT_WRAPPER_TOKEN:-"complement-$$-$(date +%s%N)"}"
  export PASS_COMPLEMENT_WRAPPER_TOKEN="$COMPLEMENT_WRAPPER_TOKEN"
  export COMPLEMENT_SHARE_ENV_PREFIX=PASS_

  cleanup_complement_containers() {
    local containers container ours=()
    if command -v docker &>/dev/null; then
      mapfile -t containers < <(docker ps -aq --filter "name=complement" 2>/dev/null || true)
      for container in "${containers[@]:-}"; do
        if docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$container" 2>/dev/null \
            | grep -Fxq "COMPLEMENT_WRAPPER_TOKEN=$COMPLEMENT_WRAPPER_TOKEN"; then
          ours+=("$container")
        fi
      done
      if [ "${#ours[@]}" -gt 0 ]; then
        echo "Cleaning up Complement containers spawned by this run..."
        printf '%s\n' "${ours[@]}" | xargs -r docker rm -f
      fi
    fi
  }
  trap cleanup_complement_containers EXIT

  # Ensure default container spawn timeout is generous under load
  export COMPLEMENT_SPAWN_HS_TIMEOUT_SECS=${COMPLEMENT_SPAWN_HS_TIMEOUT_SECS:-120}

  # ── record_result: one summary line + append to staged results ───────────────
  record_result() {
    local action="$1" test_name="$2" elapsed="$3"
    jq -nc --arg Action "$action" --arg Test "$test_name" \
      '{Action: $Action, Test: $Test}' >>"$staged_results_file"
    printf '%s\t%s\t%s\n' "${action^^}" "$test_name" "$elapsed"
  }

  # ── run_one_pattern: one go test invocation per -run alternative ─────────────
  run_one_pattern() {
    local pattern="$1"

    # Narrow packages to where the requested test lives.
    local -a packages
    if [ -n "$use_in_repo_tests" ]; then
      packages=("${default_in_repo_complement_test_packages[@]}")
    else
      packages=("${available_complement_test_packages[@]}")
    fi

    if [[ "$pattern" != "." ]] && [[ "$pattern" =~ ^\^?(Test[[:alnum:]_]+)(/.*)?$ ]]; then
      local _test_name="${BASH_REMATCH[1]}"
      local _base_dir="$COMPLEMENT_DIR"
      if [ -n "$use_in_repo_tests" ]; then _base_dir="${repo_root}/complement"; fi
      if command -v rg &>/dev/null; then
        local -a matched_pkgs=()
        mapfile -t matched_pkgs < <(
          cd "$_base_dir" \
            && rg -l --glob '*_test.go' "^func[[:space:]]+${_test_name}" tests 2>/dev/null \
            | xargs -r -n1 dirname | sed 's#^#./#' | sort -u || true
        )
        if [ "${#matched_pkgs[@]}" -gt 0 ]; then
          packages=("${matched_pkgs[@]}")
          echo "Selected package(s) for $pattern: ${packages[*]}"
        fi
      fi
    fi

    local -a flags=(
      -tags "$test_tags"
      -v
      -count=1
      -timeout "$test_timeout"
      -p "$test_parallel"
      -parallel "$test_parallel"
      "${extra_args[@]}"
    )
    if [[ "$pattern" != "." ]]; then flags+=(-run "$pattern"); fi

    local _events_dir
    _events_dir="$(mktemp -d "${staged_results_file}.events.XXXXXX")"
    local _events_fifo="${_events_dir}/events"
    mkfifo "$_events_fifo"

    local _go_exit=0
    set +e
    (
      set -o pipefail
      if [ -n "$use_in_repo_tests" ]; then
        cd "${repo_root}/complement"
      else
        cd "$COMPLEMENT_DIR"
      fi
      go test -json "${flags[@]}" "${packages[@]}" \
        | tee -a "$staged_log_file" \
        | jq --unbuffered -r \
          'select((.Action == "pass" or .Action == "fail" or .Action == "skip") and .Test != null)
           | (.Elapsed // 0) as $e
           | [.Action, .Test,
              (if $e == 0 then "0s"
               else ((($e * 100 | round) / 100) | tostring) + "s" end)
             ] | @tsv' \
        >"$_events_fifo"
    ) &
    local _producer=$!

    while IFS=$'\t' read -r _action _tname _elapsed; do
      [ -n "$_action" ] || continue
      record_result "$_action" "$_tname" "$_elapsed"
    done <"$_events_fifo"

    wait "$_producer"
    _go_exit=$?
    set -e
    rm -rf "$_events_dir"
    return "$_go_exit"
  }

  # ── Run all patterns ──────────────────────────────────────────────────────────
  local test_start_seconds=$SECONDS
  local go_test_exit_code=0

  for _pattern in "${ALT_PATTERNS[@]}"; do
    set +e
    run_one_pattern "$_pattern"
    local _pexit=$?
    set -e
    if [ "$_pexit" -ne 0 ]; then go_test_exit_code="$_pexit"; fi
  done

  echo "DEBUG: tests done, go_test_exit_code=$go_test_exit_code" >&2

  # ── Merge / refresh results ledger ────────────────────────────────────────────
  local merge_script="${repo_root}/scripts-dev/merge_complement_results.py"
  echo "DEBUG: staged_results_file=$staged_results_file exists=$( [ -f "$staged_results_file" ] && echo yes || echo no ) size=$( [ -f "$staged_results_file" ] && wc -c <"$staged_results_file" || echo 0 )" >&2
  if [ -f "$staged_results_file" ] && [ -s "$staged_results_file" ]; then
    if [ "$RUN_TESTS" = "." ]; then
      # Full run: dedupe + sort staged, then replace main results outright.
      python3 "$merge_script" --dedupe-in-place "$staged_results_file" \
        || echo "WARN: dedupe failed; keeping raw rows" >&2
      python3 "$merge_script" --sort-in-place "$staged_results_file" \
        || echo "WARN: sort failed; keeping arrival order" >&2
      cp "$staged_results_file" "$main_results_file"
      echo "Refreshed $main_results_file from $(wc -l <"$staged_results_file" | tr -d ' ') results"
    else
      # Partial run: merge new results into ledger.
      local tmp_merge
      tmp_merge="$(mktemp "${main_results_file}.merge.XXXXXX")"
      if python3 "$merge_script" "$main_results_file" "$staged_results_file" "$tmp_merge"; then
        mv "$tmp_merge" "$main_results_file"
        echo "Merged $(wc -l <"$staged_results_file" | tr -d ' ') results into $main_results_file"
      else
        echo "WARN: merge failed; appending staged results" >&2
        cat "$staged_results_file" >>"$main_results_file"
        rm -f "$tmp_merge"
      fi
    fi
  elif [ -f "$staged_results_file" ]; then
    echo "Warning: $staged_results_file exists but is empty" >&2
  else
    echo "Warning: $staged_results_file is missing" >&2
  fi

  # Log: point-in-time snapshot, straight copy (not merge).
  if [ -f "$staged_log_file" ]; then
    cp "$staged_log_file" "$main_log_file"
  fi

  local test_duration_seconds=$((SECONDS - test_start_seconds))

  # Benchmark every run: print a clearly greppable duration line for local
  # trend-watching, and add it to the GitHub Actions job summary when
  # running in CI so each run's duration is visible/browsable in the
  # Actions UI without any extra CLI archaeology.
  #
  # In CI, do NOT print to stdout/stderr: this script's combined
  # stdout+stderr is piped (via `2>&1 | tee ... | ...`) into a log file that
  # a downstream step feeds straight to `gotestfmt` for strict
  # `go test -json` parsing (see .github/workflows/complement_tests.yml's
  # "Sanity check Complement image" / "Run Complement Tests" steps). A
  # stray non-JSON line there breaks gotestfmt's parser (exit code 2) even
  # though go test itself passed -- unlike that workflow's own `jq`
  # progress filter, which explicitly tolerates non-JSON lines, gotestfmt
  # does not. $GITHUB_STEP_SUMMARY is a separate file untouched by that
  # pipe, so it's always safe.
  echo ""
  echo ""
  echo "complement logs saved at $staged_log_file"
  echo "complement results staged at $staged_results_file"
  echo "complement results merged into $main_results_file"
  echo ""
  echo ""

  if [ -z "${GITHUB_ACTIONS:-}" ]; then
    echo "COMPLEMENT_DURATION_SECONDS=${test_duration_seconds}"
  fi
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
      echo "### Complement duration"
      echo "\`${test_duration_seconds}s\` (in_repo=\`${use_in_repo_tests:-0}\`)"
    } >> "$GITHUB_STEP_SUMMARY"
  fi

  return "$go_test_exit_code"
}

main "$@"
# For any non-zero exit code (indicating some sort of error happened), we want to exit
# with that code.
exit_code=$?
if [ $exit_code -ne 0 ]; then
    exit $exit_code
fi
