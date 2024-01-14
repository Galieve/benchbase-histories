# shellcheck disable=SC2164
cd target/benchbase-postgres
# shellcheck disable=SC2164
mkdir -p experiments
mkdir -p results/testFiles

# shellcheck disable=SC2103

# shellcheck disable=SC2103


END=100
EXAMPLES=5
executeBenchmark () {

    name=$1
    isolationCase=$2
    shift
    shift
    isolations=( "$@" )
    

    rm -rf "experiments/${name}/${isolationCase}"
    mkdir -p "experiments/${name}/${isolationCase}"

    for i in $(seq 1 $END); do

        cp "config/postgres/sample_${name}Histories_config.xml" "experiments/${name}/${isolationCase}/${name}Histories-${i}_config.xml"
        xmlstarlet ed -L -u "/parameters/works/work/rate" -v "$i" "experiments/${name}/${isolationCase}/${name}Histories-${i}_config.xml"


        for (( j=1; j<${#isolations[@]} ; j+=2 )) ; do
            transaction=${isolations[j-1]}
            isolation=${isolations[j]}
            #echo xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\""${transaction}"\"]" -t elem -n "isolation" -v "$isolation" "experiments/${name}/${isolationCase}/${name}Histories-${i}_config.xml"

            xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\""${transaction}"\"]" -t elem -n "isolation" -v "$isolation" "experiments/${name}/${isolationCase}/${name}Histories-${i}_config.xml"
        done
        
        # \
             # -i "//similarity" -t attr -n "class" -v "solr.BM25SimilarityFactory"  \
        for j in $(seq 1 $EXAMPLES); do


            rm -rf "results/testFiles/${name}/${isolationCase}/case-${i}(${j})"
            mkdir -p "results/testFiles/${name}/${isolationCase}/case-${i}(${j})"
            touch "results/testFiles/${name}/${isolationCase}/case-${i}(${j})/output.out"

            echo java -jar benchbase.jar -b "${name}Histories" -c "experiments/${name}/${isolationCase}/${name}Histories-${i}_config.xml" -d "results/testFiles/${name}/${isolationCase}/case-${i}(${j})" --create=true --load=true --execute=true
            java &> "results/testFiles/${name}/${isolationCase}/case-${i}(${j})/output.out" -jar benchbase.jar -b "${name}Histories" -c "experiments/${name}/${isolationCase}/${name}Histories-${i}_config.xml" -d "results/testFiles/${name}/${isolationCase}/case-${i}(${j})" --create=true --load=true --execute=true
        done
    done

}

executeBenchmarkWeights () {
    bch=$1
    name=$2
    rm -rf "experiments/${name}"
    mkdir -p "experiments/${name}"

    for i in $(seq 1 $END); do
        cp "config/postgres/sample_${bch}Histories_config.xml" "experiments/${name}/${name}Histories-${i}_config.xml"
        #<weights>1,1,7,90,1</weights>
        xmlstarlet ed -L -u /parameters/works/work/rate -v "$i" "experiments/${name}/${name}Histories-${i}_config.xml"
        xmlstarlet ed -L -u /parameters/works/work/weights -v "1,1,7,10,81" "experiments/${name}/${name}Histories-${i}_config.xml"

        for j in $(seq 1 $EXAMPLES); do


            rm -rf "results/testFiles/${name}/case-${i}(${j})"
            mkdir -p "results/testFiles/${name}/case-${i}(${j})"
            touch "results/testFiles/${name}/case-${i}(${j})/output.out"

            echo java -jar benchbase.jar -b "${bch}Histories" -c "experiments/${name}/$/${name}Histories-${i}_config.xml" -d "results/testFiles/${name}/case-${i}(${j})" --create=true --load=true --execute=true
            #java &> "results/testFiles/${name}/case-${i}(${j})/output.out" -jar benchbase.jar -b "${bch}Histories" -c "experiments/${name}/${name}Histories-${i}_config.xml" -d "results/testFiles/${name}/case-${i}(${j})" --create=true --load=true --execute=true
        done
    done

}

#executeBenchmarkWeights "twitter" "twitterHigh" "1,1,7,10,81"
#executeBenchmarkWeights "twitter" "twitterMedium" "1,1,7,40,51"
#executeBenchmark "twitter"
executeBenchmark "tpccND" "SER" "OrderStatusNDHistory" "TRANSACTION_SERIALIZABLE" "DeliveryNDHistory" "TRANSACTION_SERIALIZABLE" "StockLevelNDHistory" "TRANSACTION_SERIALIZABLE" "NewOrderNDHistory" "TRANSACTION_SERIALIZABLE" "PaymentNDHistory" "TRANSACTION_SERIALIZABLE"
executeBenchmark "tpcc" "SER" "OrderStatusHistory" "TRANSACTION_SERIALIZABLE" "DeliveryHistory" "TRANSACTION_SERIALIZABLE" "StockLevelHistory" "TRANSACTION_SERIALIZABLE" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE"

executeBenchmark "tpccND" "SER+RC" "OrderStatusNDHistory" "TRANSACTION_READ_COMMITTED" "DeliveryNDHistory" "TRANSACTION_READ_COMMITTED" "StockLevelNDHistory" "TRANSACTION_READ_COMMITTED" "NewOrderNDHistory" "TRANSACTION_SERIALIZABLE" "PaymentNDHistory" "TRANSACTION_SERIALIZABLE"
executeBenchmark "tpcc" "SER+RC" "OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_SERIALIZABLE" "PaymentHistory" "TRANSACTION_SERIALIZABLE"

executeBenchmark "tpccND" "SI+RC" "OrderStatusNDHistory" "TRANSACTION_READ_COMMITTED" "DeliveryNDHistory" "TRANSACTION_READ_COMMITTED" "StockLevelNDHistory" "TRANSACTION_READ_COMMITTED" "NewOrderNDHistory" "TRANSACTION_REPEATABLE_READ" "PaymentNDHistory" "TRANSACTION_REPEATABLE_READ"
executeBenchmark "tpcc" "SI+RC" "OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_REPEATABLE_READ" "PaymentHistory" "TRANSACTION_REPEATABLE_READ"

executeBenchmark "tpccND" "RC" "OrderStatusNDHistory" "TRANSACTION_READ_COMMITTED" "DeliveryNDHistory" "TRANSACTION_READ_COMMITTED" "StockLevelNDHistory" "TRANSACTION_READ_COMMITTED" "NewOrderNDHistory" "TRANSACTION_READ_COMMITTED" "PaymentNDHistory" "TRANSACTION_READ_COMMITTED"
executeBenchmark "tpcc" "RC" "OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED" "NewOrderHistory" "TRANSACTION_READ_COMMITTED" "PaymentHistory" "TRANSACTION_READ_COMMITTED"

#executeBenchmark "twitter" "RC" "GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_READ_COMMITTED"
#executeBenchmark "twitter" "SER+RC" "GetTweetHistory" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingHistory" "TRANSACTION_READ_COMMITTED" "GetFollowersHistory" "TRANSACTION_READ_COMMITTED" "GetUserTweetsHistory" "TRANSACTION_READ_COMMITTED" "InsertTweetHistory" "TRANSACTION_SERIALIZABLE"
#executeBenchmark "tpcc" "SER+RC" "OrderStatusHistory" "TRANSACTION_READ_COMMITTED" "DeliveryHistory" "TRANSACTION_READ_COMMITTED" "StockLevelHistory" "TRANSACTION_READ_COMMITTED"
#executeBenchmark "tpccND"