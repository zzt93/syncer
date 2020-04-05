#!/usr/bin/env bash

source cases/test-normal.sh

# shellcheck disable=SC2034
env=drds-es7
num=100
syncerDir=normal-new-es

source "${UTIL_LIB}"

function test-new-es() {
    docker stop syncer
    # Given
    bash script/generate_data.sh ${num} ${env}
    bash script/load_data.sh ${env}

    docker start syncer
    # Given
    bash script/generate_data.sh ${num} ${env} $(( MYSQL_INSTANCE * num ))
    bash script/load_data.sh ${env}

    waitSyncer $num

    # Then: sync to mysql
    cmpFromTo extractMySqlCount extractMySqlResultCount

    # Then: sync to new es
    cmpFromTo extractConst extractES7Count $(( 2 * MYSQL_INSTANCE * num ))
    # Then: test clear
    cmpFromTo extractConst extractES7Count 0 discard
    # Then: test copy
    all=$(( 2 * 2 * MYSQL_INSTANCE * num ))
    cmpFromTo extractConst extractES7Count ${all} copy

    assertLogNotExist syncer ' ERROR '
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  setup
  test-new-es
  cleanup
fi
