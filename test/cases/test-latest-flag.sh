#!/usr/bin/env bash

env=drds
num=100
syncerDir=latest

source ${UTIL_LIB}


function setup() {
    configEnvVar ${env}
    bash script/setupEnv_new.sh ${env} ${syncerDir}
}

function test-latest() {
    docker stop syncer
    # Given
    bash script/generateData.sh ${num} ${env}
    bash script/loadData.sh ${env}

    docker start syncer
    # Given
    bash script/generateData.sh ${num} ${env} ${num}
    bash script/loadData.sh ${env}

    # Then: count == num
    cmpFromTo extractConst extractESCount ${num}
}


function cleanup() {
    cleanupAll
}


setup
test-latest
cleanup
