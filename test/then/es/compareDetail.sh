#!/usr/bin/env bash

env=$1
idMax=$2

if [[ -z ${idMax} ]]; then
    idMax=100
fi

source ${LOG_LIB}


logi "-----"
logi "Testing $0"
logi "-----"


function esCompare() {
    instance=$1
    db=$2
    table=$3

    s=`shuf -i 0-$(($idMax/2)) -n 1`
    e=`shuf -i ${s}-${idMax} -n 1`
    ids="0"
    for ((id=s;id<=e;id++)); do
        ids="$ids,$id"
    done

    docker-compose -f ${ENV_CONFIG} exec ${instance} mysql -uroot -proot -B -e "select * from ${db}.${table} where id >= $s and id <= $e" > $PWD/then/es/mysql_${table}
    curl -s -X GET "localhost:49200/${db}*/${table}/_search" -H 'Content-Type: application/json' -d "
    {
        \"query\": {
            \"ids\" : {
                \"values\" : [$ids]
            }
        }
    }"  > $PWD/then/es/es_${table}

    logi "Compare ${instance}.${db}.${table}"
    python $PWD/then/es/compareDetail.py $PWD/then/es/es_${table} $PWD/then/es/mysql_${table}
}


# tables in mysql_test.sql
names="news correctness types"
for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
    for table in ${names} ; do
        esCompare mysql_${i} test_${i} ${table}
    done
done
esCompare mysql_0 simple simple_type


logi "-----"
logi "Done $0"
logi "-----"
