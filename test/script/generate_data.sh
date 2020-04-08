#!/usr/bin/env bash
set -e

source ${UTIL_LIB}

lines=$1
env=$2
start=$3
reGenerate=$4

if [[ -z ${reGenerate} ]]; then
    reGenerate=true
fi

if [[ -z ${start} ]]; then
    start=0
fi

function generateMysqlTestData() {
    logi "---------------------"
    logi "generateMysqlTestData"
    logi "---------------------"

    lines=$1
    start=$2
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        for db in ${allDBs} ; do
            mkdir -p data/mysql/${i}/csv/${db}
            exists=`find data/mysql/${i}/csv/${db} -name '*.csv'`
        done
        if [[ -z "$exists" || ${reGenerate} = true ]]; then
            for db in ${allDBs} ; do
                # @see const.sh
                sqlFile="${db}.sql"
                cp generator/${sqlFile} data/
                docker run -v "$(pwd)"/data:/data --rm generator:test /data/mysql/${i} /data/${sqlFile} ${lines} ${start} 4  >> "${LOG_FILE}"
            done
        fi
        start=$(($start + ${lines}))
    done

    cd ${TEST_DIR}
}

int='^[0-9]+$'
function generateMongoTestData() {
    logi "---------------------"
    logi "generateMongoTestData"
    logi "---------------------"

    start=$2

    mkdir -p ${TEST_DIR}/data/mongo/
    cd ${TEST_DIR}/../syncer-core/
    if ! [[ "$start" =~ $int ]]; then
      mvn test -q -Dtest=com.github.zzt93.syncer.test.MongoGenerator -DargLine="-Dnum=$1 -DfileName=${TEST_DIR}/data/mongo/simple_type.json" >> "${LOG_FILE}"
    else
      mvn test -q -Dtest=com.github.zzt93.syncer.test.MongoGenerator -DargLine="-Dnum=$1 -Dstart=$start -DfileName=${TEST_DIR}/data/mongo/simple_type.json" >> "${LOG_FILE}"
    fi
    cd ${TEST_DIR}
}

if [[ ${env} = "mongo" || ${env} = "mongo_v4" ]]; then
    generateMongoTestData ${lines} ${start}
else
    generateMysqlTestData ${lines} ${start}
fi
