#!/usr/bin/env bash

set -e

case=$1
build=$2

function buildSyncer() {
    if [[ ${build} ]]; then
        # package syncer
        mvn clean package

        # build syncer image
        #docker rmi -f syncer:test
        docker build syncer-core -t syncer:test
    fi
}

function configEnvVar() {
    cd test

    export LOG_LIB=`pwd`/script/log.sh
    source ${LOG_LIB}
    export UTIL_LIB=`pwd`/script/util.sh

    export TEST_DIR=`pwd`
    export CONFIG_DIR=`pwd`/config
    export TEST_DATA_DIR=`pwd`/data

    export LOG_FILE=`pwd`/test-$(date +'%Y-%m-%d %H:%M:%S%z').log
    touch "${LOG_FILE}"
}

buildSyncer
configEnvVar


if [[ -z ${case} ]]; then
    for c in `find cases -name "*.sh"` ; do
        logi "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        logi "Testing ${c}"
        logi "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        bash ${c}
        res=$?
        if [[ ${res} -eq 77 ]]; then
            exit 77
        fi
        logi "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
        logi "Tested ${c}"
        logi "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~"
    done
else
    bash cases/${case}
fi