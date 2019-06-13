#!/usr/bin/env bash
set -e

source ${UTIL_LIB}


function generateMysqlTestData() {
    logi "---------------------"
    logi "generateMysqlTestData"
    logi "---------------------"

    cd generator
    docker build . -f DataGenerator.Dockerfile -t generator:test
    cd ..
    start=0
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        for f in generator/*.sql; do
            filename=`basename ${f}`
            dir=${filename%".sql"}
            mkdir -p data/mysql/${i}/csv/${dir}
            exists=`find data/mysql/${i}/csv/${dir} -name '*.csv'`
        done
        if [[ -z "$exists" ]]; then

            for f in generator/*.sql; do
                name=`basename ${f}`
                docker run -v $(pwd)/data:/data --rm generator:test /data/mysql/${i} /${name} $1 ${start} 4
            done
        fi
        start=$(($start + $1))
    done

    cd ${TEST_DIR}
}

function generateMongoTestData() {
    logi "---------------------"
    logi "generateMongoTestData"
    logi "---------------------"

    mkdir -p ${TEST_DIR}/data/mongo/
    cd ${TEST_DIR}/../syncer-core/
    mvn test -q -Dtest=com.github.zzt93.syncer.common.data.MongoGenerator -DargLine="-Dnum=$1 -DfileName=${TEST_DIR}/data/mongo/test.json"
    mvn test -q -Dtest=com.github.zzt93.syncer.common.data.MongoGenerator -DargLine="-Dnum=$1 -DfileName=${TEST_DIR}/data/mongo/test2.json"
    cd ${TEST_DIR}
}


function generateInitSqlFile() {
    logi "-------------------"
    logi "generateInitSqlFile"
    logi "-------------------"

    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        tmp="data/mysql_init_${i}.sql"
        if [[ ! -e ${tmp} ]];then
            echo -e "CREATE DATABASE IF NOT EXISTS test_$i;\n use test_$i;" > ${tmp}
            cat generator/mysql_test.sql >> ${tmp}
            cat generator/mysql_simple.sql >> ${tmp}
        fi
        export mysql_init_${i}=$(pwd)/${tmp}
    done
}


function prepareEnv() {
    logi "----------------"
    logi " docker-compose "
    logi "----------------"
    docker-compose -f ${ENV_CONFIG} up -d

    dockerExec mongo mongo --eval "rs.initiate()"
}


lines=$1
env=$2


if [[ ${env} = "mongo" ]]; then
    generateMongoTestData ${lines}
else
    generateMysqlTestData ${lines}
    generateInitSqlFile
fi
prepareEnv