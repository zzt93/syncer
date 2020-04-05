#!/usr/bin/env bash

env=drds
num=100
syncerDir=normal

source ${UTIL_LIB}


function setup() {
    configEnvVar ${env}
    bash script/setup_env_new.sh ${env} ${syncerDir}
}


function test-non-latest() {
    # Given
    bash script/generate_data.sh ${num} ${env}
    bash script/load_data.sh ${env}

    instance=mysql_0
    dockerExec ${instance} mysql -uroot -proot -N -B -e "alter table test_0.news add yy char(10) default 'aa' null after plate_type; "
    dockerExec mysql_1 mysql -uroot -proot -N -B -e "alter table test_1.news add yy char(10) default 'aa' null after plate_type; "

    # Given
    bash script/generate_data.sh ${num} ${env} ${num}
    bash script/load_data.sh ${env}

    dockerExec ${instance} mysql -uroot -proot -N -B -e "alter table test_0.news modify yy char(101) default 'aa' null; "

    # Given
    bash script/generate_data.sh ${num} ${env} $(( 2 * num ))
    bash script/load_data.sh ${env}

    dockerExec ${instance} mysql -uroot -proot -N -B -e "alter table test_0.news drop column yy; "

    # Given
    bash script/generate_data.sh ${num} ${env} $(( 3 * num ))
    bash script/load_data.sh ${env}

    waitSyncer $num

    # Then: sync to es
    cmpFromTo extractMySqlCount extractESCount
    # Then: sync to mysql
    cmpFromTo extractMySqlCount extractMySqlResultCount

    # Then: test clear
    cmpFromTo extractConst extractESCount 0 discard
    # Then: test copy
    all=$(( 8 * num ))
    cmpFromTo extractConst extractESCount ${all} copy

    assertLogNotExist syncer ' ERROR '
}

function cleanup() {
    cleanupAll
}

setup
test-non-latest
cleanup
