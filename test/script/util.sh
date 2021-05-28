#!/usr/bin/env bash

source ${LOG_LIB}
source ${CONST}

function dockerExec() {
  local service="$1"; shift
  docker exec -i $(docker-compose -f ${ENV_CONFIG} ps -q ${service}) "$@"
}

function extractESCount() {
    local instance=$1
    local db=$2
    local table=$3

    local response=`curl -s -X GET "localhost:49200/${db}*/${table}/_count" -H 'Content-Type: application/json'`
    logi ${response} | egrep -o '\{"count":[0-9]+' | awk -F ':' '{print $NF}'
}


function extractMySqlCount() {
    local instance=$1
    local db=$2
    local table=$3

    dockerExec ${instance} mysql -uroot -proot -N -B -e "select count(*) from ${db}.${table}" | grep -o "[0-9]*"
}


export mysqlResultSuffix="_bak"

function extractMySqlResultCount() {
    local instance=$1
    local db=$2
    local table="$3$mysqlResultSuffix"

    extractMySqlCount ${instance} ${db} ${table}
}


function extractMongoCount() {
    local instance=$1
    local db=$2
    local table=$3

    dockerExec mongo mongo ${db} --quiet --eval "db.${table}.count()"
}



function generateInitSqlFile() {
    logi "-------------------"
    logi "generateInitSqlFile"
    logi "-------------------"

    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        tmp="data/mysql_init_${i}.sql"
        if [[ ! -e ${tmp} ]];then
            for db in ${allDBs} ; do
                echo -e "CREATE DATABASE IF NOT EXISTS ${db}_$i;\n use ${db}_$i;" >> ${tmp}
                cat generator/${db}.sql >> ${tmp}
            done
        fi
        export mysql_init_${i}=$(pwd)/${tmp}
    done
    # backup instance has same db/table
    for (( i = 0; i < ${MYSQL_BAK_INSTANCE}; ++i )); do
        tmp="data/mysql_init_${i}_bak.sql"
        if [[ ! -e ${tmp} ]];then
            for db in ${allDBs} ; do
                echo -e "CREATE DATABASE IF NOT EXISTS ${db}_$i;\n use ${db}_$i;" >> ${tmp}
                cat generator/${db}.sql >> ${tmp}
            done
        fi
        export mysql_init_${i}_bak=$(pwd)/${tmp}
    done
    # only put backup table in first database
    for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
        tmp="data/mysql_init_0.sql"
        for db in ${allDBs} ; do
            echo -e "CREATE DATABASE IF NOT EXISTS ${db}_$i;\n use ${db}_$i;" >> ${tmp}
            cat generator/${db}.sql >> ${tmp}
        done
    done
}


function configEnvVar() {
    env=$1
    if [[ ${env} == mysql* ]]; then
        export MYSQL_INSTANCE=1
    elif [[ ${env} == drds* ]]; then
        export MYSQL_INSTANCE=3
    elif [[ ${env} == mongo* ]]; then
        export MYSQL_INSTANCE=0
    else
        loge "Unsupported env"
        exit 1
    fi

    if [[ ${env} == mysql-bak ]]; then
        export MYSQL_BAK_INSTANCE=1
    else
        export MYSQL_BAK_INSTANCE=0
    fi

    export ENV_CONFIG=$(pwd)/docker-compose/${env}.yml

    if [[ ${env} != mongo* ]]; then
        generateInitSqlFile
    fi
}

function waitSyncer() {
    times=$1
    if [[ -z ${times} ]]; then
        times=3
    fi
    for (( i = 0; i < $times; ++i )); do
        printf "Waiting Syncer to warm up .\r"
        sleep 1
        printf "Waiting Syncer to warm up ..\r"
        sleep 1
        printf "Waiting Syncer to warm up ...\r"
        sleep 1
        echo ""
    done
}

function extractConst() {
    echo $4
}


function cmpFromTo() {
    waitSyncer

    fromF=$1
    toF=$2
    expected=$3
    dbs=$4
    if [[ -z ${dbs} ]]; then
        dbs=${defaultDBs}
    fi
    tableSel=$5

    hasError=false
    if [[ ${MYSQL_INSTANCE} = 0 ]]; then
        instance=mongo
        i=0
        for dbPrefix in ${dbs} ; do
            db=${dbPrefix}_${i}
            for table in ${db2table[${dbPrefix}]} ; do
              if [[ -n $tableSel ]] && [[ $table = $tableSel ]]; then
                from=`${fromF} ${instance} ${db} ${table} ${expected}`
                logi "[Sync input]: $fromF ${instance} ${db}.${table} ${expected}: $from"
                # instance is only used by DRDS test case, and target instance is always mysql_0, see drds.yml & sync config
                to=`${toF} mysql_0 ${db} ${table} ${expected}`
                logi "[Sync result]: $toF mysql_0 ${db} ${table} ${expected}: $to"
                if [[ ${to} -ne "$from" || ${to} -eq 0 ]];then
                    loge "$table not right"
                    hasError=true
                fi
              fi
            done
        done
    else
        for (( i = 0; i < ${MYSQL_INSTANCE}; ++i )); do
            instance=mysql_${i}
            for dbPrefix in ${dbs} ; do
                db=${dbPrefix}_${i}
                for table in ${db2table[${dbPrefix}]} ; do
                  if [[ -n $tableSel ]] && [[ $table = $tableSel ]]; then
                    from=`${fromF} ${instance} ${db} ${table} ${expected}`
                    logi "[Sync input: $fromF] ${instance} ${db}.${table} ${expected}: $from"
                    # instance is only used by DRDS test case, and target instance is always mysql_0, see drds.yml & sync config
                    to=`${toF} mysql_0 ${db} ${table} ${expected}`
                    logi "[Sync result: $toF] mysql_0 ${db} ${table} ${expected}: $to"
                    if [[ ${to} -ne "$from" ]];then
                        loge "$table not right"
                        hasError=true
                    fi
                  fi
                done
            done
        done
    fi


    if [[ ${hasError} = true ]]; then
        exit 77
    fi
}

function detail() {
  idMin=$1
  idMax=$2
  input=$3
  output=$4

  logi "---------------------"
  logi "cmp detail"
  logi "---------------------"
  cd ${TEST_DIR}/../syncer-core/
  mvn test -q -Dtest=com.github.zzt93.syncer.test.CompareDetail -DargLine="-DidMin=$idMin -DidMax=$idMax -Dinput=$input -Doutput=$output"
  if [[ $? != 0 ]]; then
      exit 77
  fi
  cd ${TEST_DIR}
}

function cleanupAll() {
    docker-compose -f docker-compose/${env}.yml rm -fsv
    rm -rf data/*
}


function assertLogNotExist() {
    container=$1
    msg=$2
    res=`docker logs ${container} | grep -v 'Invalid config for' | grep -v 'No such repo' | grep "$msg"`
    if [[ -n "${res}" ]]; then
        exit 77
    fi
}