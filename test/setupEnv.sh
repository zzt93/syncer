#!/usr/bin/env bash
set -e

source ${LOG_LIB}


function generateMysqlTestData() {
    logi "---------------------"
    logi "generateMysqlTestData"
    logi "---------------------"

    cd generator
    docker build . -f DataGenerator.Dockerfile -t generator:test
    cd ..
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
                docker run -v $(pwd)/data:/data --rm generator:test /data/mysql/${i} /${name} $1
            done
        fi
    done


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

function loadToMysql() {
    logi "----------------"
    logi "  loadToMysql   "
    logi "----------------"

    cd data/mysql
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        cd ${i}
        for f in `find csv/mysql_test -name "*.csv"`; do
            if [[ ${f} != *_bak.csv ]]; then
                docker-compose -f ${ENV_CONFIG} exec mysql_${i} mysqlimport --fields-terminated-by=, --verbose --local -u root -proot test_${i} /Data/mysql/${i}/${f}
            fi
        done
        cd ..
    done
    docker-compose -f ${ENV_CONFIG} exec mysql_0 mysqlimport --fields-terminated-by=, --verbose --local -u root -proot simple /Data/mysql/0/csv/mysql_simple/simple_type.csv
    cd ../..
}

function prepareEnv() {
    logi "----------------"
    logi " docker-compose "
    logi "----------------"
    docker-compose -f ${ENV_CONFIG} up -d
}


lines=$1
env=$2



generateMysqlTestData ${lines}
generateInitSqlFile
prepareEnv
loadToMysql