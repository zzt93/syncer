#!/usr/bin/env bash

env=drds
num=100
syncerDir=cold

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
    # cold(latest) vs no cold
    cmpFromTo extractConst extractESCount ${num} correctness
    cmpFromTo extractConst extractMySqlResultCount ${num} correctness

    # Given
    bash script/generate_data.sh ${num} ${env} ${num}
    bash script/load_data.sh ${env}
    # cold(latest) vs no cold
    cmpFromTo extractConst extractESCount $(( 2 * num )) correctness
    cmpFromTo extractConst extractMySqlResultCount $(( 2 * num )) correctness

    # Given
    bash script/generate_data.sh ${num} ${env} $(( 2 * num ))
    bash script/load_data.sh ${env}

    # Given
    bash script/generate_data.sh ${num} ${env} $(( 3 * num ))
    bash script/load_data.sh ${env}

    # Then: sync to es
    cmpFromTo extractConst extractESCount $(( 4 * num )) correctness
    cmpFromTo extractConst extractESCount $(( 3 * num )) types
    cmpFromTo extractConst extractESCount $(( 3 * num )) news
    cmpFromTo extractConst extractESCount $(( 3 * num )) simple_type
    # Then: sync to mysql
    cmpFromTo extractMySqlCount extractMySqlResultCount

    assertLogNotExist syncer ' ERROR '
}

function cleanup() {
    cleanupAll
}

setup
test-non-latest
cleanup
