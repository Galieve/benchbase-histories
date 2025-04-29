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

EXAMPLES=1
END_COMPARISON=10


executeBenchmark () {

    local name=$1
    local optionsFolderName=$2
    local isolationCase=$3
    local end=$4
    local -n options=$5
    local -n isolations=$6

    for i in $(seq 1 $end); do

        mkdir -p "results/config/${optionsFolderName}/${name}/${isolationCase}/"

        cp "config/postgres/sample_${name}_config.xml" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
        config_file=$(echo "fileTransactions" "$name" "$optionsFolderName" "$isolationCase" "$i" "${isolations[@]}")

        for j in $(seq 1 $EXAMPLES); do

            #rm -rf "results/testFiles/${name}/${isolationCase}/case-${i}(${j})"
            mkdir -p "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})"

            if [ "${CLEAR_OLD_EXPERIMENTS}" == 'true' ]; then
                rm -rf "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/"*
            fi

            touch "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out"

            file="results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out"

            args_run=$(echo \
                        "-b" "${name}" \
                        "-c" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" \
                        "-d" "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" \
                        "${options[@]}" \
                        "--create=true" "--load=true" "--execute=true")

            args="${file};${args_run};${config_file}"


            SKIP_TESTS=${SKIP_TESTS:-true} EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
            ./docker/benchbase/run-artifact-image.sh \
                "$args"
        done
    done

}




executeTwitter() {


    local -a algorithms=("-ch" "\"Naive,CSOB\"" "-di" "\"GetTweetHistory,TRANSACTION_SERIALIZABLE,GetTweetsFromFollowingHistory,TRANSACTION_SERIALIZABLE,GetFollowersHistory,TRANSACTION_SERIALIZABLE,GetUserTweetsHistory,TRANSACTION_SERIALIZABLE,InsertTweetHistory,TRANSACTION_SERIALIZABLE\"")
    local -a isolationMap=("GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "twitterHistories" "Comparison" "Naive-vs-CheckSOBound" $END_COMPARISON algorithms isolationMap


    ./docker/benchbase/run-artifact-image.sh \
        "python;generate_csv.py twitter Comparison '' $END_COMPARISON false"



}

executeTPCC() {

    local -a algorithms=("-ch" "\"Naive,CSOB\"" "-di" "\"OrderStatusHistory,TRANSACTION_SERIALIZABLE,DeliveryHistory,TRANSACTION_SERIALIZABLE,StockLevelHistory,TRANSACTION_SERIALIZABLE,NewOrderHistory,TRANSACTION_SERIALIZABLE,PaymentHistory,TRANSACTION_SERIALIZABLE\"")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_READ_COMMITTED" "PaymentHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "tpccHistories" 'Comparison' "Naive-vs-CheckSOBound" $END_COMPARISON algorithms isolationMap


    ./docker/benchbase/run-artifact-image.sh \
        "python;generate_csv.py tpcc Comparison '' $END_COMPARISON false"


}

executeTwitter
executeTPCC




