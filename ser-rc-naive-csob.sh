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


    #rm -rf "experiments/${name}/${isolationCase}"
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
        #         # -i "//similarity" -t attr -n "class" -v "solr.BM25SimilarityFactory"  \
        for j in $(seq 1 $EXAMPLES); do

            #rm -rf "results/testFiles/${name}/${isolationCase}/case-${i}(${j})"
            mkdir -p "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})"
            touch "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out"

            echo java -jar benchbase.jar -b "${name}" -c "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" -d "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" "${options[@]}" --create=true --load=true --execute=true
            echo
            java &> "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out" -jar benchbase.jar -b "${name}" -c "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" -d "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" "${options[@]}" --create=true --load=true --execute=true

        done
    done

}
#executeBenchmark "twitter" "SER+RC" "GetTweetFull" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingFull" "TRANSACTION_READ_COMMITTED" "GetFollowersFull" "TRANSACTION_READ_COMMITTED" "GetUserTweetsFull" "TRANSACTION_READ_COMMITTED" "InsertTweetFull" "TRANSACTION_SERIALIZABLE"
#executeBenchmark "twitter" "SI+RC" "GetTweetFull" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingFull" "TRANSACTION_READ_COMMITTED" "GetFollowersFull" "TRANSACTION_READ_COMMITTED" "GetUserTweetsFull" "TRANSACTION_READ_COMMITTED" "InsertTweetFull" "TRANSACTION_REPEATABLE_READ"
declare -a options=("-ch" "\"Naive, CSOB\"" "-di" "\"GetTweetHistory TRANSACTION_SERIALIZABLE GetTweetsFromFollowingHistory TRANSACTION_SERIALIZABLE GetFollowersHistory TRANSACTION_SERIALIZABLE GetUserTweetsHistory TRANSACTION_SERIALIZABLE InsertTweetHistory TRANSACTION_SERIALIZABLE\"")
declare -a isolations=("GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_READ_COMMITTED")

executeBenchmark "twitterHistories" "Invalid" "Naive-vs-CheckSOBound" "${options[@]}" "${isolations[@]}"

#options=("-ch" "CSOB" "-di" "\"GetTweetHistory TRANSACTION_SERIALIZABLE GetTweetsFromFollowingHistory TRANSACTION_SERIALIZABLE GetFollowersHistory TRANSACTION_SERIALIZABLE GetUserTweetsHistory TRANSACTION_SERIALIZABLE InsertTweetHistory TRANSACTION_SERIALIZABLE\"")


#executeBenchmark "twitterHistories""Invalid" "CheckSOBound" "${options[@]}" "${isolations[@]}"

options=("-ch" "\"Naive, CSOB\"" "-di" "\"OrderStatusHistory TRANSACTION_SERIALIZABLE DeliveryHistory TRANSACTION_SERIALIZABLE StockLevelHistory TRANSACTION_SERIALIZABLE NewOrderHistory TRANSACTION_SERIALIZABLE PaymentHistory TRANSACTION_SERIALIZABLE\"")
isolations=("OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_READ_COMMITTED" "PaymentHistory" "TRANSACTION_READ_COMMITTED")


executeBenchmark "tpccHistories" "Invalid" "Naive-vs-CheckSOBound" "${options[@]}" "${isolations[@]}"

#options=("-ch" "CSOB" "-di" "\"OrderStatusHistory TRANSACTION_SERIALIZABLE DeliveryHistory TRANSACTION_SERIALIZABLE StockLevelHistory TRANSACTION_SERIALIZABLE NewOrderHistory TRANSACTION_SERIALIZABLE PaymentHistory TRANSACTION_SERIALIZABLE\"")

#executeBenchmark "tpccHistories" "Invalid" "CheckSOBound" "${options[@]}" "${isolations[@]}"
