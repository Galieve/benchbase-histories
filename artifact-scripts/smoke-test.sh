#!/bin/bash

CLEAR_OLD_EXPERIMENTS='true'

preamble() {
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
}
preamble

EXAMPLES=1
END_SESSION=3
END_TRANSACTION=5
END_COMPARISON=3

executeBenchmark () {

    local name=$1
    local optionsFolderName=$2
    local isolationCase=$3
    local end=$4
    local -n options=$5
    local -n isolations=$6
    local commandFiles=$7


    for i in $(seq 1 $end); do

        mkdir -p "results/config/${optionsFolderName}/${name}/${isolationCase}/"

        cp "config/postgres/sample_${name}_config.xml" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
        $commandFiles "$name" "$optionsFolderName" "$isolationCase" "$i"

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
                rm -rf "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/"*
            fi

            touch "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out"

            echo ./docker/benchbase/run-artifact-image.sh \
                                 -b "${name}" \
                                 -c "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" \
                                 -d "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" \
                                 "${options[@]}" --create=true --load=true --execute=true


            #SKIP_TESTS=${SKIP_TESTS:-true} EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
            #./docker/benchbase/run-artifact-image.sh \

             #./docker/benchbase/run-full-image.sh \
                        #    &> "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out" \

            #java \
            #    &> "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out" \
            #    -jar ./profiles/${BENCHBASE_PROFILE}/benchbase.jar \
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

fileSessions() {
    local name=$1
    local optionsFolderName=$2
    local isolationCase=$3
    local i=$4
    xmlstarlet ed -L -u "/parameters/works/work/rate" -v "10" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
    xmlstarlet ed -L -u "/parameters/terminals" -v "$i" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
    xmlstarlet ed -L -u "/parameters/works/work/time" -v "$i" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
}

fileTransactions() {
    local name=$1
    local optionsFolderName=$2
    local isolationCase=$3
    local i=$4
    xmlstarlet ed -L -u "/parameters/works/work/rate" -v "$i" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
}

executeTPCCSessions() {

    local -a algorithms=("-ch" "CSOB")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_REPEATABLE_READ" "PaymentHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccHistories" "Smoke-Test-Sessions" "SI+RC" $END_SESSION algorithms isolationMap "fileSessions"

    isolationMap=("OrderStatusHistory" "TRANSACTION_SERIALIZABLE" "DeliveryHistory" "TRANSACTION_SERIALIZABLE" "StockLevelHistory" "TRANSACTION_SERIALIZABLE" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE")
    executeBenchmark "tpccHistories" "Smoke-Test-Sessions" "SER" $END_SESSION algorithms isolationMap "fileSessions"

    source .venv/bin/activate && cd graphics && python3 generate_csv.py 'tpcc' 'Smoke-Test-Sessions' "SER,SI+RC" $END_SESSION 'true' && cd ..
    source .venv/bin/activate && cd graphics && python3 graphics.py 'tpcc' 'Smoke-Test-Sessions' "SER,SI+RC" $END_SESSION && cd ..


}


executeTPCCTransactions() {

    local -a algorithms=("-ch" "CSOB")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_REPEATABLE_READ" "PaymentHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccHistories" "Smoke-Test-Transactions" "SI+RC" $END_TRANSACTION algorithms isolationMap "fileTransactions"

    isolationMap=("OrderStatusHistory" "TRANSACTION_SERIALIZABLE" "DeliveryHistory" "TRANSACTION_SERIALIZABLE" "StockLevelHistory" "TRANSACTION_SERIALIZABLE" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE")
    executeBenchmark "tpccHistories" "Smoke-Test-Transactions" "SER" $END_TRANSACTION algorithms isolationMap "fileTransactions"


    source .venv/bin/activate && cd graphics && python3 generate_csv.py 'tpcc' 'Smoke-Test-Transactions' "SER,SI+RC" $END_TRANSACTION 'true' && cd ..
    source .venv/bin/activate && cd graphics && python3 graphics.py 'tpcc' 'Smoke-Test-Transactions' "SER,SI+RC" $END_TRANSACTION && cd ..


}

executeTPCCComparisons() {

    local -a algorithms=("-ch" "\"Naive,CSOB\"" "-di" "\"OrderStatusHistory,TRANSACTION_SERIALIZABLE,DeliveryHistory,TRANSACTION_SERIALIZABLE,StockLevelHistory,TRANSACTION_SERIALIZABLE,NewOrderHistory,TRANSACTION_SERIALIZABLE,PaymentHistory,TRANSACTION_SERIALIZABLE\"")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_READ_COMMITTED" "PaymentHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "tpccHistories" "Smoke-Test-Comparisons" "Naive-vs-CheckSOBound" $END_COMPARISON algorithms isolationMap "fileTransactions"

    source .venv/bin/activate && cd graphics && python3 generate_csv.py 'tpcc' 'Smoke-Test-Comparisons' "" $END_COMPARISON 'false' && cd ..

}

executeTPCCSessions
executeTPCCTransactions
executeTPCCComparisons



