#!/bin/bash

CLEAR_OLD_EXPERIMENTS='true'


set -eu
set -x

# Let these pass through from the .env file from the devcontainer.
export BENCHBASE_PROFILE="postgres"
export BENCHBASE_PROFILES="$BENCHBASE_PROFILE"
PROFILE_VERSION=${PROFILE_VERSION:-latest}

# When we are running the artifact image we don't generally want to have to rebuild it repeatedly.
export CLEAN_BUILD="${CLEAN_BUILD:-false}"
export BUILD_IMAGE="false"


# Move to the repo root.
scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/..")
cd "$rootdir"

EXTRA_DOCKER_ARGS=''

bash "./docker/${BENCHBASE_PROFILE}-${PROFILE_VERSION}/up.sh"


CREATE_DB_ARGS='--create=true --load=true'
if [ "${SKIP_LOAD_DB:-false}" == 'true' ]; then
    CREATE_DB_ARGS=''
fi


SKIP_TESTS=${SKIP_TESTS:-true} EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
  ./docker/benchbase/run-artifact-image.sh