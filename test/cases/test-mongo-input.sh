#!/usr/bin/env bash

env=mongo
num=100
syncerDir=mongo-input

source ${UTIL_LIB}


function setup() {
    configEnvVar ${env}
    bash script/setupEnv_new.sh ${env} ${syncerDir}
}


function test-mongo-input() {
    docker stop syncer
    # Given
    bash script/generateData.sh ${num} ${env}
    bash script/loadData.sh ${env}

    docker start syncer
    # Given
    bash script/generateData.sh ${num} ${env} ${num}
    bash script/loadData.sh ${env}

    # Then: count == num * 2
    cmpFromTo extractMongoCount extractESCount
}

function cleanup() {
    cleanupAll
}

setup
test-mongo-input
cleanup
