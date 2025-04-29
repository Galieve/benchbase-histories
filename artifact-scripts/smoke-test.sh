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
END_COMPARISON=2




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
        config_file=$(echo "$commandFiles" "$name" "$optionsFolderName" "$isolationCase" "$i" "${isolations[@]}")

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

            args="benchbase;${file};${args_run};${config_file}"


            SKIP_TESTS=${SKIP_TESTS:-true} EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
                ./docker/benchbase/run-artifact-image.sh "$args"
        done
    done

}

executeTPCCSessions() {

    local -a algorithms=("-ch" "CSOB")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_REPEATABLE_READ" "PaymentHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccHistories" "Smoke-Test-Sessions" "SI+RC" $END_SESSION algorithms isolationMap "fileSessions"

    isolationMap=("OrderStatusHistory" "TRANSACTION_SERIALIZABLE" "DeliveryHistory" "TRANSACTION_SERIALIZABLE" "StockLevelHistory" "TRANSACTION_SERIALIZABLE" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE")
    executeBenchmark "tpccHistories" "Smoke-Test-Sessions" "SER" $END_SESSION algorithms isolationMap "fileSessions"


    ./docker/benchbase/run-artifact-image.sh "python;generate_csv.py tpcc Smoke-Test-Sessions SER,SI+RC $END_SESSION true"
    ./docker/benchbase/run-artifact-image.sh "python;graphics.py tpcc Smoke-Test-Sessions SER,SI+RC sessions $END_SESSION"

    #source .venv/bin/activate && cd graphics && python3 generate_csv.py 'tpcc' 'Smoke-Test-Sessions' "SER,SI+RC" $END_SESSION 'true' && cd ..
    #source .venv/bin/activate && cd graphics && python3 graphics.py 'tpcc' 'Smoke-Test-Sessions' "SER,SI+RC" "sessions" $END_SESSION && cd ..


}


executeTPCCTransactions() {

    local -a algorithms=("-ch" "CSOB")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_REPEATABLE_READ" "PaymentHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccHistories" "Smoke-Test-Transactions" "SI+RC" $END_TRANSACTION algorithms isolationMap "fileTransactions"

    isolationMap=("OrderStatusHistory" "TRANSACTION_SERIALIZABLE" "DeliveryHistory" "TRANSACTION_SERIALIZABLE" "StockLevelHistory" "TRANSACTION_SERIALIZABLE" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE")
    executeBenchmark "tpccHistories" "Smoke-Test-Transactions" "SER" $END_TRANSACTION algorithms isolationMap "fileTransactions"

    ./docker/benchbase/run-artifact-image.sh "python;generate_csv.py tpcc Smoke-Test-Transactions SER,SI+RC $END_TRANSACTION true"
    ./docker/benchbase/run-artifact-image.sh "python;graphics.py tpcc Smoke-Test-Transactions SER,SI+RC transactions $END_TRANSACTION"

    #source .venv/bin/activate && cd graphics && python3 generate_csv.py 'tpcc' 'Smoke-Test-Transactions' "SER,SI+RC" $END_TRANSACTION 'true' && cd ..
    #source .venv/bin/activate && cd graphics && python3 graphics.py 'tpcc' 'Smoke-Test-Transactions' "SER,SI+RC" "transactions" $END_TRANSACTION && cd ..


}

executeTPCCComparisons() {

    local -a algorithms=("-ch" "\"Naive,CSOB\"" "-di" "\"OrderStatusHistory,TRANSACTION_SERIALIZABLE,DeliveryHistory,TRANSACTION_SERIALIZABLE,StockLevelHistory,TRANSACTION_SERIALIZABLE,NewOrderHistory,TRANSACTION_SERIALIZABLE,PaymentHistory,TRANSACTION_SERIALIZABLE\"")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_READ_COMMITTED" "PaymentHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "tpccHistories" "Smoke-Test-Comparisons" "Naive-vs-CheckSOBound" $END_COMPARISON algorithms isolationMap "fileTransactions"

    ./docker/benchbase/run-artifact-image.sh "python;generate_csv.py tpcc Smoke-Test-Comparisons '' $END_COMPARISON false"

}

executeTPCCSessions
executeTPCCTransactions
executeTPCCComparisons



