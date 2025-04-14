#!/bin/bash

set -eu
set -x
export BENCHBASE_PROFILE='postgres'
# Optional additional overrides (defaults shown):

# Set which profiles to build.
export BENCHBASE_PROFILES="$BENCHBASE_PROFILE"
# Specify a different version of the profile to use (suffix in this directory).
export PROFILE_VERSION='latest'
# Whether or not to rebuild the package/image.
export CLEAN_BUILD="false"
# When rebuilding, whether or not to run the unit tests.
export SKIP_TESTS="true"


./docker/benchbase/build-artifact-image.sh
