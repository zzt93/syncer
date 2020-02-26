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
        for db in ${allDBs} ; do
            for f in `find csv/${db} -name "*.csv"`; do
                dockerExec mysql_${i} mysqlimport --defaults-file=/Data/my.cnf --fields-terminated-by=, --verbose --local ${db}_${i} /Data/mysql/${i}/${f} >> "${LOG_FILE}"
            done
        done
        cd ..
    done

    logi "loading sql"
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        cd ${i}
        for db in ${allDBs} ; do
            for f in `find sql/${db} -name "*.sql"`; do
                dockerExec mysql_${i} mysql --defaults-file=/Data/my.cnf ${db}_${i} < ${f} >> "${LOG_FILE}"
            done
        done
        cd ..
    done

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
        dockerExec mongo mongoimport --db simple_0 --collection ${col} --file /Data/mongo/${f} --jsonArray >> "${LOG_FILE}"
    done

    cd ${TEST_DIR}
}

env=$1


if [[ ${env} = "mongo" || ${env} = "mongo_v4" ]]; then
    loadToMongo
else
    loadToMysql
fi
