#!/bin/bash

CLEAR_OLD_EXPERIMENTS='true'



EXAMPLES=1
END_TRANSACTION=18

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

    local -a options=("-ch" "CSOB")
    local -a isolationMap=("GetTweetHistory" "TRANSACTION_SERIALIZABLE" "GetTweetsFromFollowingHistory" "TRANSACTION_SERIALIZABLE" "GetFollowersHistory" "TRANSACTION_SERIALIZABLE" "GetUserTweetsHistory" "TRANSACTION_SERIALIZABLE" "InsertTweetHistory" "TRANSACTION_SERIALIZABLE")

    executeBenchmark "twitterHistories" "Transaction-Scalability" "SER" $END_TRANSACTION algorithms isolationMap

    isolationMap=("GetTweetHistory" "TRANSACTION_REPEATABLE_READ" "GetTweetsFromFollowingHistory" "TRANSACTION_REPEATABLE_READ" "GetFollowersHistory" "TRANSACTION_REPEATABLE_READ" "GetUserTweetsHistory" "TRANSACTION_REPEATABLE_READ" "InsertTweetHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "twitterHistories" "Transaction-Scalability" "SI" $END_TRANSACTION algorithms isolationMap

    isolationMap=("GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "twitterHistories" "Transaction-Scalability" "RC" $END_TRANSACTION algorithms isolationMap

    isolationMap=("GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "twitterHistories" "Transaction-Scalability" "SI+RC" $END_TRANSACTION algorithms isolationMap

    isolationMap=("GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_SERIALIZABLE")

    executeBenchmark "twitterHistories" "Transaction-Scalability" "SER+RC" $END_TRANSACTION algorithms isolationMap

    source .venv/bin/activate && cd graphics && \
        python3 generate_csv.py 'twitter' 'Transaction-Scalability' "SER,SI,RC,SER+RC,SI+RC" $END_TRANSACTION 'true' \
        && cd ..
    source .venv/bin/activate && cd graphics && \
        python3 graphics.py 'twitter' 'Transaction-Scalability' "SER,SI,RC,SER+RC,SI+RC" $END_TRANSACTION \
        && cd ..

}

executeTPCC() {

    local -a options=("-ch" "CSOB")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_SERIALIZABLE" "DeliveryHistory" "TRANSACTION_SERIALIZABLE" "StockLevelHistory" "TRANSACTION_SERIALIZABLE" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE")

    executeBenchmark "tpccHistories" "Transaction-Scalability" "SER" $END_TRANSACTION algorithms isolationMap

    isolationMap=("OrderStatusHistory" "TRANSACTION_REPEATABLE_READ" "DeliveryHistory" "TRANSACTION_REPEATABLE_READ" "StockLevelHistory" "TRANSACTION_REPEATABLE_READ" "NewOrderHistory" "TRANSACTION_REPEATABLE_READ" "PaymentHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccHistories" "Transaction-Scalability" "SI" $END_TRANSACTION algorithms isolationMap

    isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_READ_COMMITTED" "PaymentHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "tpccHistories" "Transaction-Scalability" "RC" $END_TRANSACTION algorithms isolationMap

    isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_REPEATABLE_READ" "PaymentHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccHistories" "Transaction-Scalability" "SI+RC" $END_TRANSACTION algorithms isolationMap

    isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE")

    executeBenchmark "tpccHistories" "Transaction-Scalability" "SER+RC" $END_TRANSACTION algorithms isolationMap

    source .venv/bin/activate && cd graphics && \
        python3 generate_csv.py 'tpcc' 'Transaction-Scalability' "SER,SI,RC,SER+RC,SI+RC" $END_TRANSACTION 'true' \
        && cd ..
    source .venv/bin/activate && cd graphics && \
        python3 graphics.py 'tpcc' 'Transaction-Scalability' "SER,SI,RC,SER+RC,SI+RC" $END_TRANSACTION \
        && cd ..

}

executeTPCCPC() {

    local -a options=("-ch" "CSOB")
    local -a isolationMap=("OrderStatusPCHistory" "TRANSACTION_SERIALIZABLE" "DeliveryPCHistory" "TRANSACTION_SERIALIZABLE" "StockLevelPCHistory" "TRANSACTION_SERIALIZABLE" "NewOrderPCHistory" "TRANSACTION_SERIALIZABLE" "PaymentPCHistory" "TRANSACTION_SERIALIZABLE")
    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "SER" $END_TRANSACTION algorithms isolationMap

    isolationMap=("OrderStatusPCHistory" "TRANSACTION_REPEATABLE_READ" "DeliveryPCHistory" "TRANSACTION_REPEATABLE_READ" "StockLevelPCHistory" "TRANSACTION_REPEATABLE_READ" "NewOrderPCHistory" "TRANSACTION_REPEATABLE_READ" "PaymentPCHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "SI" $END_TRANSACTION algorithms isolationMap

    isolationMap=("OrderStatusPCHistory" "TRANSACTION_READ_COMMITTED" "DeliveryPCHistory" "TRANSACTION_READ_COMMITTED" "StockLevelPCHistory" "TRANSACTION_READ_COMMITTED" "NewOrderPCHistory" "TRANSACTION_READ_COMMITTED" "PaymentPCHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "RC" $END_TRANSACTION algorithms isolationMap

    isolationMap=("OrderStatusPCHistory" "TRANSACTION_READ_COMMITTED" "DeliveryPCHistory" "TRANSACTION_READ_COMMITTED" "StockLevelPCHistory" "TRANSACTION_READ_COMMITTED" "NewOrderPCHistory" "TRANSACTION_REPEATABLE_READ" "PaymentPCHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "SI+RC" $END_TRANSACTION algorithms isolationMap

    isolationMap=("OrderStatusPCHistory" "TRANSACTION_READ_COMMITTED" "DeliveryPCHistory" "TRANSACTION_READ_COMMITTED" "StockLevelPCHistory" "TRANSACTION_READ_COMMITTED" "NewOrderPCHistory" "TRANSACTION_SERIALIZABLE" "PaymentPCHistory" "TRANSACTION_SERIALIZABLE")

    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "SER+RC" $END_TRANSACTION algorithms isolationMap

    source .venv/bin/activate && cd graphics && \
        python3 generate_csv.py 'tpccPC' 'Transaction-Scalability' "SER,SI,RC,SER+RC,SI+RC" $END_TRANSACTION 'true' \
        && cd ..
    source .venv/bin/activate && cd graphics && \
        python3 graphics.py 'tpccPC' 'Transaction-Scalability' "SER,SI,RC,SER+RC,SI+RC" $END_TRANSACTION \
        && cd ..

}

executeTwitter
executeTPCC
executeTPCCPC




