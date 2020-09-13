#!/usr/bin/env bash

env=mysql-bak
num=100
syncerDir=simplest

source ${UTIL_LIB}


function setup() {
    configEnvVar ${env}
    bash script/setup_env_new.sh ${env} ${syncerDir}
}


function test-non-latest() {
    # Given
    bash script/generate_data.sh ${num} ${env}
    bash script/load_data.sh ${env}

    dockerExec mysql_1 mysql -uroot -proot -N -B -e "alter table test_1.news add yy char(10) default 'aa' null after plate_type; "
    instance=mysql_0
    dockerExec ${instance} mysql -uroot -proot -N -B -e "alter table test_0.news add yy char(10) default 'aa' null after plate_type; "

    # Given
    bash script/generate_data.sh ${num} ${env} ${num}
    bash script/load_data.sh ${env}

    waitSyncer 60

    # Then: sync to es
    cmpFromTo extractMySqlCount extractESCount
    # Then: sync to mysql
    cmpFromTo extractMySqlCount extractMySqlResultCount2

    assertLogNotExist syncer ' ERROR '

    detail 0 ${num} mysql_0 es
    detail 0 ${num} mysql_0 mysql_0
}

function cleanup() {
    cleanupAll
}

setup
test-non-latest
cleanup
