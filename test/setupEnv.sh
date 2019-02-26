#!/usr/bin/env bash


function generateTestData() {
    echo "----------------"
    echo "generateTestData"
    echo "----------------"

    mkdir -p data/csv
    for f in generator/*.sql; do
        filename=`basename ${f}`
        dir=${filename%".sql"}
        exists=`find data/csv/${dir} -name '*.csv'`
    done

    if [[ -z "$exists" ]]; then
        cd generator
        docker build . -f DataGenerator.Dockerfile -t generator:test
        cd ..

        for f in generator/*.sql; do
            name=`basename ${f}`
            docker run -v $(pwd)/data:/data --rm generator:test /${name} $1
        done
    fi
}

function generateInitSqlFile() {
    echo "-------------------"
    echo "generateInitSqlFile"
    echo "-------------------"

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
    echo "----------------"
    echo "  loadToMysql   "
    echo "----------------"

    mysql_instance=$1
    cd data
    for (( i = 0; i < mysql_instance; ++i )); do
        for f in `find csv -name "*.csv"`; do
            if [[ ${f} != *_bak.csv ]]; then
                docker-compose -f "../$env.yml" exec mysql_${i} mysqlimport --fields-terminated-by=, --verbose --local -u root -proot test_${i} /tmp/${f}
            fi
        done
    done
}

function prepareEnv() {
    echo "----------------"
    echo " docker-compose "
    echo "----------------"
    docker-compose -f "$env.yml" up -d
}

function checkParameter() {
    if [[ ${env} = "mysql" ]]; then
        mysql_instance=1
    elif [[ ${env} = "drds" ]]; then
        mysql_instance=3
    else
        echo "Unsupported env"
        exit 1
    fi
}

lines=$1
env=$2



checkParameter

generateTestData ${lines}
generateInitSqlFile ${mysql_instance}
prepareEnv
loadToMysql ${mysql_instance}