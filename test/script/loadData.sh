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
            dockerExec mysql_${i} mysqlimport --fields-terminated-by=, --verbose --local -u root -proot test_${i} /Data/mysql/${i}/${f}
        done
        cd ..
    done
    dockerExec mysql_0 mysqlimport --fields-terminated-by=, --verbose --local -u root -proot simple /Data/mysql/0/csv/mysql_simple/simple_type.csv

    logi "loading sql"
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        cd ${i}
        for f in `find sql/mysql_test -name "*.sql"`; do
            dockerExec mysql_${i} mysql -u root -proot test_${i} < ${f}
        done
        cd ..
    done
    dockerExec mysql_0  mysql -u root -proot simple < 0/sql/mysql_simple/simple_type*.sql

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
        dockerExec mongo mongoimport --db mongo-test --collection ${col} --file /Data/mongo/${f} --jsonArray
    done

    cd ${TEST_DIR}
}

env=$1


if [[ ${env} = "mongo" ]]; then
    loadToMongo
else
    loadToMysql
fi
