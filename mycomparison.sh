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
    options=$2
    optionsFolderName=$3
    isolationCase=$4

    shift
    shift
    shift
    shift
    isolations=( "$@" )


    #rm -rf "experiments/${name}/${isolationCase}"
    mkdir -p "experiments/${optionsFolderName}/${name}/${isolationCase}"

    for i in $(seq 1 $END); do

        cp "config/postgres/sample_${name}_config.xml" "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
        xmlstarlet ed -L -u "/parameters/works/work/rate" -v "$i" "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"


        for (( j=1; j<${#isolations[@]} ; j+=2 )) ; do
            transaction=${isolations[j-1]}
            isolation=${isolations[j]}
            #echo xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\""${transaction}"\"]" -t elem -n "isolation" -v "$isolation" "experiments/${name}/${isolationCase}/${name}-${i}_config.xml"

            xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\"""${transaction}""\"]" -t elem -n "isolation" -v "$isolation" "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
        done

        # \
             # -i "//similarity" -t attr -n "class" -v "solr.BM25SimilarityFactory"  \
        for j in $(seq 1 $EXAMPLES); do


            #rm -rf "results/testFiles/${name}/${isolationCase}/case-${i}(${j})"
            mkdir -p "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})"
            touch "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out"

            echo java -jar benchbase.jar -b "${name}" -c "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" -d "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" "${options}" --create=true --load=true --execute=true
            java &> "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})/output.out" -jar benchbase.jar -b "${name}" -c "experiments/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml" -d "results/testFiles/${optionsFolderName}/${name}/${isolationCase}/case-${i}(${j})" "${options}" --create=true --load=true --execute=true
        done
    done

}
#executeBenchmark "twitterFull" "-kv" "key-value" "SER" "GetTweetFull" "TRANSACTION_SERIALIZABLE" "GetTweetsFromFollowingFull" "TRANSACTION_SERIALIZABLE" "GetFollowersFull" "TRANSACTION_SERIALIZABLE" "GetUserTweetsFull" "TRANSACTION_SERIALIZABLE" "InsertTweetFull" "TRANSACTION_SERIALIZABLE"
#executeBenchmark "twitter" "" "predicates" "SER"  "GetTweet" "TRANSACTION_SERIALIZABLE" "GetTweetsFromFollowing" "TRANSACTION_SERIALIZABLE" "GetFollowers" "TRANSACTION_SERIALIZABLE" "GetUserTweets" "TRANSACTION_SERIALIZABLE" "InsertTweet" "TRANSACTION_SERIALIZABLE"
#executeBenchmark "twitterFull" "" "predicates-full" "SER"  "GetTweetFull" "TRANSACTION_SERIALIZABLE" "GetTweetsFromFollowingFull" "TRANSACTION_SERIALIZABLE" "GetFollowersFull" "TRANSACTION_SERIALIZABLE" "GetUserTweetsFull" "TRANSACTION_SERIALIZABLE" "InsertTweetFull" "TRANSACTION_SERIALIZABLE"
#executeBenchmark "twitterFull" "-kv" "key-value" "SI" "GetTweetFull" "TRANSACTION_REPEATABLE_READ" "GetTweetsFromFollowingFull" "TRANSACTION_REPEATABLE_READ" "GetFollowersFull" "TRANSACTION_REPEATABLE_READ" "GetUserTweetsFull" "TRANSACTION_REPEATABLE_READ" "InsertTweetFull" "TRANSACTION_REPEATABLE_READ"
#executeBenchmark "twitter" "" "predicates" "SI"  "GetTweet" "TRANSACTION_REPEATABLE_READ" "GetTweetsFromFollowing" "TRANSACTION_REPEATABLE_READ" "GetFollowers" "TRANSACTION_REPEATABLE_READ" "GetUserTweets" "TRANSACTION_REPEATABLE_READ" "InsertTweet" "TRANSACTION_REPEATABLE_READ"
#executeBenchmark "twitterFull" "" "predicates-full" "SI" "GetTweetFull" "TRANSACTION_REPEATABLE_READ" "GetTweetsFromFollowingFull" "TRANSACTION_REPEATABLE_READ" "GetFollowersFull" "TRANSACTION_REPEATABLE_READ" "GetUserTweetsFull" "TRANSACTION_REPEATABLE_READ" "InsertTweetFull" "TRANSACTION_REPEATABLE_READ"

#executeBenchmark "twitter" "SER+RC" "GetTweetFull" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingFull" "TRANSACTION_READ_COMMITTED" "GetFollowersFull" "TRANSACTION_READ_COMMITTED" "GetUserTweetsFull" "TRANSACTION_READ_COMMITTED" "InsertTweetFull" "TRANSACTION_SERIALIZABLE"
#executeBenchmark "twitter" "SI+RC" "GetTweetFull" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingFull" "TRANSACTION_READ_COMMITTED" "GetFollowersFull" "TRANSACTION_READ_COMMITTED" "GetUserTweetsFull" "TRANSACTION_READ_COMMITTED" "InsertTweetFull" "TRANSACTION_REPEATABLE_READ"
#executeBenchmark "twitter" "RC" "GetTweetFull" "TRANSACTION_READ_COMMITTED" "GetTweetsFromFollowingFull" "TRANSACTION_READ_COMMITTED" "GetFollowersFull" "TRANSACTION_READ_COMMITTED" "GetUserTweetsFull" "TRANSACTION_READ_COMMITTED" "InsertTweetFull" "TRANSACTION_READ_COMMITTED"
