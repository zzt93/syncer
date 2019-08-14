#!/usr/bin/env bash

env=drds
num=100
syncerDir=non-latest

source ${UTIL_LIB}


function setup() {
    configEnvVar ${env}
    bash script/setupEnv_new.sh ${env} ${syncerDir}
}


function test-non-latest() {
    docker stop syncer
    # Given
    bash script/generateData.sh ${num} ${env}
    bash script/loadData.sh ${env}

    docker start syncer
    # Given
    bash script/generateData.sh ${num} ${env} ${num}
    bash script/loadData.sh ${env}

    # Then: count == num * 2
    cmpFromTo extractMySqlCount extractESCount
}

function cleanup() {
    cleanupAll
}

setup
test-non-latest
cleanup
