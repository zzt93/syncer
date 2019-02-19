#!/usr/bin/env bash


function generateTestData() {
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
    instance=$1
    for (( i = 0; i < $instance; ++i )); do
        tmp="data/mysql_init_${i}.sql"
        if [[ ! -e ${tmp} ]];then
            echo -e "CREATE DATABASE IF NOT EXISTS test_$i;\n use test_$i;" > ${tmp}
            cat generator/*.sql >> ${tmp}
        fi
        export mysql_init_${i}=$(pwd)/${tmp}
    done
}

function loadToMysql() {
    instance=$1
    cd data
    for (( i = 0; i < instance; ++i )); do
        for f in `find csv -name "*.csv"`; do
            docker-compose -f "../$env.yml" exec mysql_${i} mysqlimport --fields-terminated-by=, --verbose --local -u root -proot test_${i} /tmp/${f}
        done
    done
}

num=$1
env=$2

DRDS_INSTANCE=3

if [[ ${env} = "mysql" ]]; then
    instance=1
elif [[ ${env} = "drds" ]]; then
    instance=$DRDS_INSTANCE
else
    echo "Unsupported env"
    exit 1
fi


generateTestData ${num}
generateInitSqlFile ${instance}
docker-compose -f "$env.yml" up -d
loadToMysql ${instance}