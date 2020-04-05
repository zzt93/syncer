#!/usr/bin/env bash

env=drds
num=1000
syncerDir=normal

source ${UTIL_LIB}


function setup() {
    configEnvVar ${env}
    bash script/setup_env_new.sh ${env} ${syncerDir}
}


function test-non-latest() {
    docker stop syncer
    # Given
    bash script/generate_data.sh ${num} ${env}
    bash script/load_data.sh ${env}

    docker start syncer
    # Given
    bash script/generate_data.sh ${num} ${env} ${num}
    bash script/load_data.sh ${env}

    waitSyncer $num

    # Then: sync to es
    cmpFromTo extractMySqlCount extractESCount
    # Then: sync to mysql
    cmpFromTo extractMySqlCount extractMySqlResultCount

    # Then: test clear
    cmpFromTo extractConst extractESCount 0 discard
    # Then: test copy
    all=$(( 4 * num ))
    cmpFromTo extractConst extractESCount ${all} copy

    assertLogNotExist syncer ' ERROR '
}

function cleanup() {
    cleanupAll
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  setup
  test-non-latest
  cleanup
fi
