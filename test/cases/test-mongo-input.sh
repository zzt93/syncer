#!/usr/bin/env bash

env=mongo
num=200
syncerDir=mongo-input

source ${UTIL_LIB}


function setup() {
    configEnvVar ${env}
    bash script/setup_env_new.sh ${env} ${syncerDir}
}


function test-mongo-input() {
    docker stop syncer
    # Given
    bash script/generate_data.sh ${num} ${env} notId
    bash script/load_data.sh ${env}

    docker start syncer
    # Given
    bash script/generate_data.sh ${num} ${env} 0
    bash script/load_data.sh ${env}

    # Then: count == num * 2
    cmpFromTo extractMongoCount extractESCount 0 simple

    docker restart syncer
    # Given
    bash script/generate_data.sh ${num} ${env} ${num}
    bash script/load_data.sh ${env}

    # Then: count == num * 3
    cmpFromTo extractMongoCount extractESCount 0 simple

    assertLogNotExist syncer ' ERROR '
    detail 0 600 mongo
}

function cleanup() {
    cleanupAll
}

# if it called by bash, not by source
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    setup
    test-mongo-input
    cleanup
fi

