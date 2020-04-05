#!/usr/bin/env bash

env=drds
num=100
syncerDir=latest

source ${UTIL_LIB}


function setup() {
    configEnvVar ${env}
    bash script/setup_env_new.sh ${env} ${syncerDir}
}

function test-latest() {
    docker stop syncer
    # Given
    bash script/generate_data.sh ${num} ${env}
    bash script/load_data.sh ${env}

    docker start syncer
    # Given
    bash script/generate_data.sh ${num} ${env} ${num}
    bash script/load_data.sh ${env}

    waitSyncer $num

    # Then: count == num
    cmpFromTo extractConst extractESCount ${num}

    assertLogNotExist syncer ' ERROR '
}


function cleanup() {
    cleanupAll
}


setup
test-latest
cleanup
