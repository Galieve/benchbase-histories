#!/bin/bash

# shellcheck disable=SC2164
cd target/benchbase-postgres
# shellcheck disable=SC2164
mkdir -p experiments
mkdir -p results/testFiles

# shellcheck disable=SC2103

# shellcheck disable=SC2103


END=20
EXAMPLES=5

executeBenchmark () {

    #echo "${args_space}"


    local name=$1
    local optionsFolderName=$2
    local isolationCase=$3
    local -n options=$4
    local -n isolations=$5

    mkdir -p "experiments/${optionsFolderName}/${name}/${isolationCase}"

    for i in $(seq 1 $END); do

        cp "config/postgres/sample_${name}_config.xml" "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
        xmlstarlet ed -L -u "/parameters/works/work/rate" -v "$i" "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
        for (( j=1; j<${#isolations[@]} ; j+=2 )) ; do
            transaction=${isolations[j-1]}
            isolation=${isolations[j]}
            #echo xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\"""${transaction}""\"]" -t elem -n "isolation" -v "$isolation" "experiments/${name}/${isolationCase}/${name}-${i}_config.xml"
            xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\"""${transaction}""\"]" -t elem -n "isolation" -v "$isolation" "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
        done
        #      # -i "//similarity" -t attr -n "class" -v "solr.BM25SimilarityFactory"  \
        for j in $(seq 1 $EXAMPLES); do

            #rm -rf "results/testFiles/${name}/${isolationCase}/case-${i}(${j})"
            mkdir -p "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})"
            touch "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out"

            echo java -jar benchbase.jar -b "${name}" -c "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" -d "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" "${options[@]}" --create=true --load=true --execute=true
            echo
            java &> "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out" -jar benchbase.jar -b "${name}" -c "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" -d "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" "${options[@]}" --create=true --load=true --execute=true
            exit 1
        done
    done

}

executeTwitter() {

    local -a options=("-ch" "CSOB")
    local -a isolationMap=("GetTweetHistory" "TRANSACTION_SERIALIZABLE" "GetTweetsFromFollowingHistory" "TRANSACTION_SERIALIZABLE" "GetFollowersHistory" "TRANSACTION_SERIALIZABLE" "GetUserTweetsHistory" "TRANSACTION_SERIALIZABLE" "InsertTweetHistory" "TRANSACTION_SERIALIZABLE")
    executeBenchmark "twitterHistories" "Transaction-Scalability" "SER" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("GetTweetHistory" "TRANSACTION_REPEATABLE_READ" "GetTweetsFromFollowingHistory" "TRANSACTION_REPEATABLE_READ" "GetFollowersHistory" "TRANSACTION_REPEATABLE_READ" "GetUserTweetsHistory" "TRANSACTION_REPEATABLE_READ" "InsertTweetHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "twitterHistories" "Transaction-Scalability" "SI" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_READ_COMMITTED")


    executeBenchmark "twitterHistories" "Transaction-Scalability" "RC" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "twitterHistories" "Transaction-Scalability" "SI+RC" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_SERIALIZABLE")

    executeBenchmark "twitterHistories" "Transaction-Scalability" "SER+RC" "${options[@]}" "${isolationMap[@]}"
}

executeTPCC() {

    local -a options=("-ch" "CSOB")
    local -a isolationMap=("OrderStatusHistory" "TRANSACTION_SERIALIZABLE" "DeliveryHistory" "TRANSACTION_SERIALIZABLE" "StockLevelHistory" "TRANSACTION_SERIALIZABLE" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE")
    executeBenchmark "tpccHistories" "Transaction-Scalability" "SER" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("OrderStatusHistory" "TRANSACTION_REPEATABLE_READ" "DeliveryHistory" "TRANSACTION_REPEATABLE_READ" "StockLevelHistory" "TRANSACTION_REPEATABLE_READ" "NewOrderHistory" "TRANSACTION_REPEATABLE_READ" "PaymentHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccHistories" "Transaction-Scalability" "SI" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_READ_COMMITTED" "PaymentHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "tpccHistories" "Transaction-Scalability" "RC" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_REPEATABLE_READ" "PaymentHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccHistories" "Transaction-Scalability" "SI+RC" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE")

    executeBenchmark "tpccHistories" "Transaction-Scalability" "SER+RC" "${options[@]}" "${isolationMap[@]}"
}

executeTPCCPC() {

    local -a options=("-ch" "CSOB")
    local -a isolationMap=("OrderStatusPCHistory" "TRANSACTION_SERIALIZABLE" "DeliveryPCHistory" "TRANSACTION_SERIALIZABLE" "StockLevelPCHistory" "TRANSACTION_SERIALIZABLE" "NewOrderPCHistory" "TRANSACTION_SERIALIZABLE" "PaymentPCHistory" "TRANSACTION_SERIALIZABLE")
    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "SER" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("OrderStatusPCHistory" "TRANSACTION_REPEATABLE_READ" "DeliveryPCHistory" "TRANSACTION_REPEATABLE_READ" "StockLevelPCHistory" "TRANSACTION_REPEATABLE_READ" "NewOrderPCHistory" "TRANSACTION_REPEATABLE_READ" "PaymentPCHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "SI" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("OrderStatusPCHistory" "TRANSACTION_READ_COMMITTED" "DeliveryPCHistory" "TRANSACTION_READ_COMMITTED" "StockLevelPCHistory" "TRANSACTION_READ_COMMITTED" "NewOrderPCHistory" "TRANSACTION_READ_COMMITTED" "PaymentPCHistory" "TRANSACTION_READ_COMMITTED")

    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "RC" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("OrderStatusPCHistory" "TRANSACTION_READ_COMMITTED" "DeliveryPCHistory" "TRANSACTION_READ_COMMITTED" "StockLevelPCHistory" "TRANSACTION_READ_COMMITTED" "NewOrderPCHistory" "TRANSACTION_REPEATABLE_READ" "PaymentPCHistory" "TRANSACTION_REPEATABLE_READ")

    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "SI+RC" "${options[@]}" "${isolationMap[@]}"

    isolationMap=("OrderStatusPCHistory" "TRANSACTION_READ_COMMITTED" "DeliveryPCHistory" "TRANSACTION_READ_COMMITTED" "StockLevelPCHistory" "TRANSACTION_READ_COMMITTED" "NewOrderPCHistory" "TRANSACTION_SERIALIZABLE" "PaymentPCHistory" "TRANSACTION_SERIALIZABLE")

    executeBenchmark "tpccPCHistories" "Transaction-Scalability" "SER+RC" "${options[@]}" "${isolationMap[@]}"
}

executeTwitter
executeTPCC
executeTPCCPC