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
    rm -rf "experiments/${name}"
    mkdir -p "experiments/${name}"

    for i in $(seq 1 $END); do
        cp "config/postgres/sample_${name}Histories_config.xml" "experiments/${name}/${name}Histories-${i}_config.xml"
        xmlstarlet ed -L -u /parameters/works/work/rate -v "$i" "experiments/${name}/${name}Histories-${i}_config.xml"
        for j in $(seq 1 $EXAMPLES); do


            rm -rf "results/testFiles/${name}/case-${i}(${j})"
            mkdir -p "results/testFiles/${name}/case-${i}(${j})"
            touch "results/testFiles/${name}/case-${i}(${j})/output.out"

            echo java -jar benchbase.jar -b "${name}Histories" -c "experiments/${name}/${name}Histories-${i}_config.xml" -d "results/testFiles/${name}/case-${i}(${j})" --create=true --load=true --execute=true
            java &> "results/testFiles/${name}/case-${i}(${j})/output.out" -jar benchbase.jar -b "${name}Histories" -c "experiments/${name}/${name}Histories-${i}_config.xml" -d "results/testFiles/${name}/case-${i}(${j})" --create=true --load=true --execute=true
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

            echo java -jar benchbase.jar -b "${bch}Histories" -c "experiments/${name}/${name}Histories-${i}_config.xml" -d "results/testFiles/${name}/case-${i}(${j})" --create=true --load=true --execute=true
            java &> "results/testFiles/${name}/case-${i}(${j})/output.out" -jar benchbase.jar -b "${bch}Histories" -c "experiments/${name}/${name}Histories-${i}_config.xml" -d "results/testFiles/${name}/case-${i}(${j})" --create=true --load=true --execute=true
        done
    done

}

#executeBenchmarkWeights "twitter" "twitterHigh" "1,1,7,10,81"
#executeBenchmarkWeights "twitter" "twitterMedium" "1,1,7,40,51"
#executeBenchmark "twitter"
executeBenchmark "tpcc"
executeBenchmark "tpccND"