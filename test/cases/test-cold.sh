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
    cmpFromTo extractConst extractMySqlResultCount ${num} test correctness
    cmpFromTo extractConst extractMySqlResultCount 0 test news
    cmpFromTo extractConst extractESCount ${num}

    # Given
    bash script/generate_data.sh ${num} ${env} ${num}
    bash script/load_data.sh ${env}
    # cold(latest) vs no cold
    cmpFromTo extractConst extractMySqlResultCount $(( 2 * num )) test correctness
    cmpFromTo extractConst extractESCount $(( 2 * num ))

    # Given
    bash script/generate_data.sh ${num} ${env} $(( 2 * num ))
    bash script/load_data.sh ${env}

    # Given
    bash script/generate_data.sh ${num} ${env} $(( 3 * num ))
    bash script/load_data.sh ${env}

    # Then: sync to mysql
    cmpFromTo extractConst extractMySqlResultCount $(( 4 * num )) test correctness
    cmpFromTo extractConst extractMySqlResultCount $(( 3 * num )) test types
    cmpFromTo extractConst extractMySqlResultCount $(( 3 * num )) test news
    cmpFromTo extractConst extractMySqlResultCount $(( 3 * num )) test simple_type
    # Then: sync to es
    cmpFromTo extractMySqlCount extractESCount

    assertLogNotExist syncer ' ERROR '
}

function cleanup() {
    cleanupAll
}

setup
test-non-latest
cleanup
