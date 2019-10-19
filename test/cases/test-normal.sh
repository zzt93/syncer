#!/usr/bin/env bash

env=drds
num=1000
syncerDir=normal

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

    waitSyncer 60

    # Then: sync to es
    cmpFromTo extractMySqlCount extractESCount
    # Then: sync to mysql
    cmpFromTo extractMySqlCount extractMySqlResultCount

    # Then: test clear
    cmpFromTo extractConst extractESCount 0 discard
    # Then: test copy
    all=$(( 4 * num ))
    cmpFromTo extractConst extractESCount ${all} copy
}

function cleanup() {
    cleanupAll
}

setup
test-non-latest
cleanup
