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

fileTransactions() {
    local name=$1
    local optionsFolderName=$2
    local isolationCase=$3
    local i=$4
    xmlstarlet ed -L -u "/parameters/works/work/rate" -v "$i" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
}

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
        fileTransactions "$name" "$optionsFolderName" "$isolationCase" "$i"

        for (( j=1; j<${#isolations[@]} ; j+=2 )) ; do
            transaction=${isolations[j-1]}
            isolation=${isolations[j]}
            #echo xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\"""${transaction}""\"]" -t elem -n "isolation" -v "$isolation" "results/config/${name}/${isolationCase}/${name}-${i}_config.xml"
            xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\"""${transaction}""\"]" -t elem -n "isolation" -v "$isolation" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
        done

        for j in $(seq 1 $EXAMPLES); do

            #rm -rf "results/testFiles/${name}/${isolationCase}/case-${i}(${j})"
            mkdir -p "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})"

            if [ "${CLEAR_OLD_EXPERIMENTS}" == 'true' ]; then
                rm -rf "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/*"
            fi

            touch "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out"



             echo ./docker/benchbase/run-artifact-image.sh \
                 -b "${name}" \
                 -c "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" \
                 -d "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" \
                 "${options[@]}" --create=true --load=true --execute=true


            SKIP_TESTS=${SKIP_TESTS:-true} EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
            ./docker/benchbase/run-artifact-image.sh \
                &> "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out" \
                -b "${name}" \
                -c "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" \
                -d "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" \
                "${options[@]}"  \
                --create=true --load=true --execute=true
        done
    done

}



executeTwitter() {


    local -a algorithms=("-ch" "\"Naive,CSOB\"" "-di" "\"GetTweetHistory,TRANSACTION_SERIALIZABLE,GetTweetsFromFollowingHistory,TRANSACTION_SERIALIZABLE,GetFollowersHistory,TRANSACTION_SERIALIZABLE,GetUserTweetsHistory,TRANSACTION_SERIALIZABLE,InsertTweetHistory,TRANSACTION_SERIALIZABLE\"")
    local -a isolationMap=("GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "twitterHistories" "Comparison" "Naive-vs-CheckSOBound" $END_COMPARISON algorithms isolationMap

    source .venv/bin/activate && cd graphics && \
        python3 generate_csv.py 'twitter' 'Comparison' '' $END_COMPARISON 'false' \
        && cd ..



}

executeTPCC() {

    local -a algorithms=("-ch" "\"Naive,CSOB\"" "-di" "\"OrderStatusHistory,TRANSACTION_SERIALIZABLE,DeliveryHistory,TRANSACTION_SERIALIZABLE,StockLevelHistory,TRANSACTION_SERIALIZABLE,NewOrderHistory,TRANSACTION_SERIALIZABLE,PaymentHistory,TRANSACTION_SERIALIZABLE\"")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_READ_COMMITTED" "PaymentHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "tpccHistories" 'Comparison' "Naive-vs-CheckSOBound" $END_COMPARISON algorithms isolationMap

    source .venv/bin/activate && cd graphics && \
        python3 generate_csv.py 'tpcc' 'Comparison' '' $END_COMPARISON 'false' \
        && cd ..


}

executeTwitter
executeTPCC




