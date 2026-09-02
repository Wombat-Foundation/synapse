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
  [ -n "$git_branch" ] && git_branch="b=$git_branch"
  git_tag="$(git describe --exact-match 2>/dev/null || true)"
  [ -n "$git_tag" ] && git_tag="t=$git_tag"
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

  # * -count=1: Only run tests once, and disable caching for tests.
  # * -v: Output test logs, even if those tests pass.
  # * -tags=synapse_blacklist: Enable the `synapse_blacklist` build tag, which is
  #   necessary for `runtime.Synapse` checks/skips to work in the tests
  test_args=(
    -v
    -tags="synapse_blacklist"
    -count=1
  )

  # It takes longer than 10m to run the whole suite.
  test_args+=("-timeout=60m")

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

  # Restrict test package sweep if user specified a specific -run TestPattern
  local target_run=""
  for ((i=1; i<=$#; i++)); do
    if [[ "${!i}" == "-run" ]]; then
      next_idx=$((i+1))
      target_run="${!next_idx}"
      break
    elif [[ "${!i}" =~ ^-run=(.+) ]]; then
      target_run="${BASH_REMATCH[1]}"
      break
    fi
  done

  if [ -n "$target_run" ] && [ "$target_run" != "." ]; then
    local test_name_clean
    test_name_clean="$(echo "$target_run" | sed -E 's/^\^//; s/\$$//; s/\/.*$//')"
    if [[ "$test_name_clean" =~ ^Test[[:alnum:]_]+$ ]]; then
      local base_dir="./complement"
      if [ -z "$use_in_repo_tests" ]; then
        base_dir="$COMPLEMENT_DIR"
      fi
      if command -v rg &>/dev/null; then
        local matched_pkg
        matched_pkg="$(cd "$base_dir" && rg -l --glob '*_test.go' "^func[[:space:]]+${test_name_clean}" tests 2>/dev/null | xargs -r -n1 dirname | sed 's#^#./#' | sort -u | head -n1 || true)"
        if [ -n "$matched_pkg" ]; then
          echo "Auto-discovered test package for '$target_run': $matched_pkg"
          if [ -n "$use_in_repo_tests" ]; then
            default_in_repo_complement_test_packages=("$matched_pkg")
          else
            available_complement_test_packages=("$matched_pkg")
          fi
          local pkg_tag
          pkg_tag="$(basename "$matched_pkg")"
          if [[ "$pkg_tag" =~ ^msc[0-9]+ ]]; then
            echo "Auto-enabling build tag for $pkg_tag"
            for idx in "${!test_args[@]}"; do
              if [[ "${test_args[$idx]}" =~ ^-tags= ]]; then
                test_args[$idx]="${test_args[$idx]},$pkg_tag"
                break
              fi
            done
          fi
        fi
      fi
    fi
  fi

  # Merge any user-supplied -tags flags from "$@" into test_args, then strip
  # them from the forwarded args. Go uses the last -tags seen, so having both
  # -tags=synapse_blacklist (ours) and -tags all (user's) means ours gets
  # silently dropped. We collect all tag values into one canonical -tags= entry.
  local -a filtered_args=()
  local _i=1
  while [ $_i -le $# ]; do
    arg="${!_i}"
    if [[ "$arg" == "-tags" ]]; then
      _next=$((_i+1))
      user_tags="${!_next}"
      # Merge user tags into the existing -tags= entry in test_args
      for idx in "${!test_args[@]}"; do
        if [[ "${test_args[$idx]}" =~ ^-tags= ]]; then
          test_args[$idx]="${test_args[$idx]},${user_tags}"
          break
        fi
      done
      _i=$((_i+2))  # skip both -tags and its value
    elif [[ "$arg" =~ ^-tags=(.+) ]]; then
      user_tags="${BASH_REMATCH[1]}"
      for idx in "${!test_args[@]}"; do
        if [[ "${test_args[$idx]}" =~ ^-tags= ]]; then
          test_args[$idx]="${test_args[$idx]},${user_tags}"
          break
        fi
      done
      _i=$((_i+1))
    else
      filtered_args+=("$arg")
      _i=$((_i+1))
    fi
  done
  set -- "${filtered_args[@]}"

  # Auto-cleanup containers spawned by Complement on exit
  export COMPLEMENT_WRAPPER_TOKEN="${COMPLEMENT_WRAPPER_TOKEN:-"complement-$$-$(date +%s%N)"}"
  export PASS_COMPLEMENT_WRAPPER_TOKEN="$COMPLEMENT_WRAPPER_TOKEN"
  export COMPLEMENT_SHARE_ENV_PREFIX=PASS_

  cleanup_complement_containers() {
    local containers container ours=()
    if command -v docker &>/dev/null; then
      mapfile -t containers < <(docker ps -aq --filter "name=complement" 2>/dev/null || true)
      for container in "${containers[@]}"; do
        if docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$container" 2>/dev/null | grep -Fxq "COMPLEMENT_WRAPPER_TOKEN=$COMPLEMENT_WRAPPER_TOKEN"; then
          ours+=("$container")
        fi
      done
      if [ "${#ours[@]}" -gt 0 ]; then
        echo "Cleaning up Complement containers spawned by this run..."
        printf '%s\n' "${ours[@]}" | xargs -r docker rm -f &>/dev/null || true
      fi
    fi
  }
  trap cleanup_complement_containers EXIT

  # Ensure default container spawn timeout is generous under load
  export COMPLEMENT_SPAWN_HS_TIMEOUT_SECS=${COMPLEMENT_SPAWN_HS_TIMEOUT_SECS:-120}

  local test_start_seconds=$SECONDS
  local go_test_exit_code=0

  # Print out the executed commands so it's more obvious what's happening at the end here.
  # Things are slightly ambiguous with the in-repo vs Complement tests.
  set -x

  # Setup staged result ledger files
  local repo_root
  repo_root="$(realpath .)"
  local results_dir="${RESULTS_DIR:-tests/complement}"
  local main_results_file="$(realpath -m "$results_dir/results.jsonl")"
  local main_log_file="$(realpath -m "$results_dir/logs.jsonl")"

  mkdir -p "$(dirname "$main_results_file")"
  touch "$main_results_file" "$main_log_file"

  local run_stamp="$(date +%s%N)"
  local staging_dir="$repo_root/.tmp/complement"
  mkdir -p "$staging_dir"
  local staged_log_file="$staging_dir/logs.${run_stamp}.jsonl"
  local staged_results_file="$staging_dir/test_results.${run_stamp}.jsonl"
  : >"$staged_log_file"
  : >"$staged_results_file"

  local -a run_pkgs=()
  if [ -n "$use_in_repo_tests" ]; then
    cd "./complement"
    run_pkgs=("${default_in_repo_complement_test_packages[@]}")
  else
    cd "$COMPLEMENT_DIR"
    run_pkgs=("${available_complement_test_packages[@]}")
  fi

  if [[ " $* " =~ " -json " ]]; then
    stdbuf -oL go test -json "${test_args[@]}" "$@" "${run_pkgs[@]}" 2>&1 |
      tee -a "$staged_log_file" |
      stdbuf -oL jq -rR --unbuffered '
        . as $raw
        | (try fromjson catch $raw)
        | if type != "object" then
            $raw
          elif (.Action == "pass" or .Action == "fail" or .Action == "skip") then
            if .Test then
              "\(.Action | ascii_upcase)\t\(.Test)\t\(.Elapsed // 0)s"
            elif .Action == "fail" then
              "FAIL PACKAGE \(.Package // "<unknown>") \(.Elapsed // 0)s"
            else
              empty
            end
          else
            empty
          end' || go_test_exit_code=$?
  else
    go test -json "${test_args[@]}" "$@" "${run_pkgs[@]}" 2>&1 |
      tee -a "$staged_log_file" |
      stdbuf -oL jq -rR --unbuffered '
        . as $raw
        | (try fromjson catch $raw)
        | if type != "object" then
            empty
          elif (.Action == "pass" or .Action == "fail" or .Action == "skip") and .Test != null then
            "\(.Action | ascii_upcase)\t\(.Test)\t\(.Elapsed // 0)s"
          else
            empty
          end' || go_test_exit_code=$?
  fi

  # Extract pass/fail test records to staged_results_file
  if [ -f "$staged_log_file" ]; then
    jq -c 'select((.Action == "pass" or .Action == "fail" or .Action == "skip") and .Test != null) | {Action: .Action, Test: .Test}' "$staged_log_file" >>"$staged_results_file" 2>/dev/null || true
  fi

  # Merge staged results into main_results_file ledger
  if [ -s "$staged_results_file" ] && [ -f "$repo_root/scripts-dev/merge_complement_results.py" ]; then
    tmp_merge="$(mktemp "${main_results_file}.merge.XXXXXX")"
    if python3 "$repo_root/scripts-dev/merge_complement_results.py" "$main_results_file" "$staged_results_file" "$tmp_merge"; then
      mv "$tmp_merge" "$main_results_file"
      echo "Merged staged test results into $main_results_file"
    else
      cat "$staged_results_file" >>"$main_results_file"
      rm -f "$tmp_merge"
    fi
    cp "$staged_log_file" "$main_log_file"
  fi

  # We don't need to print out executed commands anymore
  #
  # This is just `set +x` without printing `+ set +x` to the console (via
  # https://stackoverflow.com/questions/13195655/bash-set-x-without-it-being-printed/19226038#19226038)
  { set +x; } 2>/dev/null

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
