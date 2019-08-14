#!/usr/bin/env bash


source ${UTIL_LIB}


function loadToMysql() {
    logi "----------------"
    logi "  loadToMysql   "
    logi "----------------"

    cd data/mysql

    logi "loading csv"
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        cd ${i}
        for f in `find csv/mysql_test -name "*.csv"`; do
            dockerExec mysql_${i} mysqlimport --defaults-file=/Data/my.cnf --fields-terminated-by=, --verbose --local test_${i} /Data/mysql/${i}/${f} >> "${LOG_FILE}"
        done
        cd ..
    done
    dockerExec mysql_0 mysqlimport --defaults-file=/Data/my.cnf --fields-terminated-by=, --verbose --local simple /Data/mysql/0/csv/mysql_simple/simple_type.csv >> "${LOG_FILE}"

    logi "loading sql"
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        cd ${i}
        for f in `find sql/mysql_test -name "*.sql"`; do
            dockerExec mysql_${i} mysql --defaults-file=/Data/my.cnf test_${i} < ${f} >> "${LOG_FILE}"
        done
        cd ..
    done
    dockerExec mysql_0  mysql --defaults-file=/Data/my.cnf simple < 0/sql/mysql_simple/simple_type*.sql >> "${LOG_FILE}"

    cd ${TEST_DIR}
}

function loadToMongo() {
    logi "----------------"
    logi "  loadToMongo   "
    logi "----------------"

    cd data/mongo

    logi "loading json"
    for f in `find . -name "*.json"`; do
        tmp=`basename $f`
        col=${tmp%".json"}
        dockerExec mongo mongoimport --db mongo-test --collection ${col} --file /Data/mongo/${f} --jsonArray >> "${LOG_FILE}"
    done

    cd ${TEST_DIR}
}

env=$1


if [[ ${env} = "mongo" ]]; then
    loadToMongo
else
    loadToMysql
fi
