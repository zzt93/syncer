#!/usr/bin/env bash


source ${LOG_LIB}

function checkParameter() {
    if [[ ${env} = "mysql" ]]; then
        mysql_instance=1
    elif [[ ${env} = "drds" ]]; then
        mysql_instance=3
    else
        logi "Unsupported env"
        exit 1
    fi
}


function generateMysqlTestData() {
    logi "---------------------"
    logi "generateMysqlTestData"
    logi "---------------------"

    for (( i = 0; i < ${mysql_instance}; ++i )); do
        mkdir -p data/mysql/${i}/csv
        for f in generator/*.sql; do
            filename=`basename ${f}`
            dir=${filename%".sql"}
            exists=`find data/mysql/${i}/csv/${dir} -name '*.csv'`
        done
        if [[ -z "$exists" ]]; then
            cd generator
            docker build . -f DataGenerator.Dockerfile -t generator:test
            cd ..

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

    mysql_instance=$1
    for (( i = 0; i < $mysql_instance; ++i )); do
        tmp="data/mysql_init_${i}.sql"
        if [[ ! -e ${tmp} ]];then
            echo -e "CREATE DATABASE IF NOT EXISTS test_$i;\n use test_$i;" > ${tmp}
            cat generator/*.sql >> ${tmp}
        fi
        export mysql_init_${i}=$(pwd)/${tmp}
    done
}

function loadToMysql() {
    logi "----------------"
    logi "  loadToMysql   "
    logi "----------------"

    mysql_instance=$1
    cd data/mysql
    for (( i = 0; i < mysql_instance; ++i )); do
        cd ${i}
        for f in `find csv -name "*.csv"`; do
            if [[ ${f} != *_bak.csv ]]; then
                docker-compose -f ${ENV_CONFIG} exec mysql_${i} mysqlimport --fields-terminated-by=, --verbose --local -u root -proot test_${i} /tmp/mysql/${i}/${f}
            fi
        done
        cd ..
    done
}

function prepareEnv() {
    logi "----------------"
    logi " docker-compose "
    logi "----------------"
    docker-compose -f "$env.yml" up -d
}


lines=$1
env=$2



checkParameter

generateMysqlTestData ${lines}
generateInitSqlFile ${mysql_instance}
prepareEnv
loadToMysql ${mysql_instance}