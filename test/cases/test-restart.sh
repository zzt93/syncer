#!/usr/bin/env bash

env=drds
num=100
syncerDir=normal

source ${UTIL_LIB}


function setup() {
    configEnvVar ${env}
    bash script/setup_env_new.sh ${env} ${syncerDir}
}


function test-restart() {
    # Given
    bash script/generate_data.sh ${num} ${env}
    bash script/load_data.sh ${env}

    docker restart syncer
    # Given
    bash script/generate_data.sh ${num} ${env} ${num}
    bash script/load_data.sh ${env}

    checkLog syncer Duplicate

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
test-restart
cleanup
