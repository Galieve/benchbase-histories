#!/bin/bash

# artifact docker entrypoint script

fileSessions() {
    local name=$1
    local optionsFolderName=$2
    local isolationCase=$3
    local i=$4

    local -a isolations=(${@:5})

    xmlstarlet ed -L -u "/parameters/works/work/rate" -v "10" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
    xmlstarlet ed -L -u "/parameters/terminals" -v "$i" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
    xmlstarlet ed -L -u "/parameters/works/work/time" -v "$i" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"

    for (( j=1; j<${#isolations[@]} ; j+=2 )) ; do
        transaction=${isolations[j-1]}
        isolation=${isolations[j]}
        xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\"""${transaction}""\"]" -t elem -n "isolation" -v "$isolation" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
    done
}

fileTransactions() {
    local name=$1
    local optionsFolderName=$2
    local isolationCase=$3
    local i=$4
    local -a isolations=(${@:5})
    xmlstarlet ed -L -u "/parameters/works/work/rate" -v "$i" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"

    for (( j=1; j<${#isolations[@]} ; j+=2 )) ; do
        transaction=${isolations[j-1]}
        isolation=${isolations[j]}
        xmlstarlet ed -L -s "/parameters/transactiontypes/transactiontype[name=\"""${transaction}""\"]" -t elem -n "isolation" -v "$isolation" "results/config/${optionsFolderName}/${name}/${isolationCase}/${name}-${i}_config.xml"
    done
}

oldIFS=$IFS
# Set the IFS variable to the delimiter
IFS=';'
# Split the input string using parameter expansion
newargs=($*)
# Restore the original value of IFS
IFS=$oldIFS
output_arg=${newargs[0]}
benchbase_args=${newargs[1]}
file_args=${newargs[2]}

set -eu

BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"
cd /benchbase
echo "INFO: Using environment variable BENCHBASE_PROFILE=${BENCHBASE_PROFILE} with args: $benchbase_args" >&2
if ! [ -f "profiles/${BENCHBASE_PROFILE}/benchbase.jar" ]; then
    echo "ERROR: Couldn't find profile '${BENCHBASE_PROFILE}' in container image." >&2
    exit 1
fi
cd ./profiles/${BENCHBASE_PROFILE}/
if ! [ -d results/ ] || ! [ -w results/ ]; then
    echo "ERROR: The results directory either doesn't exist or isn't writable." >&2
fi



$file_args

exec java -jar benchbase.jar &> $output_arg $benchbase_args
