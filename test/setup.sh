#!/usr/bin/env bash

num=$1
env=$2

cd generator
docker build . -f DataGenerator.Dockerfile -t generator:test
cd ..

docker run -v $(pwd)/data:/data --rm generator:test /mysql_test.sql $1
docker run -v $(pwd)/data:/data --rm generator:test /mysql_simple.sql $1

if [[ $env = "mysql" ]]; then
    tmp=`mktemp`
    echo "CREATE DATABASE IF NOT EXISTS test;\n use test;" > ${tmp}
    cat generator/mysql_test.sql >> ${tmp}
    cat generator/mysql_simple.sql >> ${tmp}
    export mysql_init=${tmp}

    docker-compose -f mysql.yml up -d
    docker-compose -f mysql.yml exec mysql mysqlimport --fields-terminated-by=, --verbose --local -u root -proot -P43306 test /tmp/mysql_test/*.csv

elif [[ $env = "drds" ]]; then
    for (( i = 0; i < 3; ++i )); do
        tmp=`mktemp`
        echo "CREATE DATABASE IF NOT EXISTS test_$i;\n use test_$i;" > $tmp
        cat generator/mysql_test.sql >> ${tmp}
        cat generator/mysql_simple.sql >> ${tmp}
        export mysql_init_${i}=${tmp}
    done

    docker-compose -f drds.yml up -d

    for (( i = 0; i < 3; ++i )); do
        port=$((43306+$i))
        docker-compose -f drds.yml exec mysql_$i mysqlimport --fields-terminated-by=, --verbose --local -u root -proot -P$port test_$i /tmp/msyql_test/*.csv
    done
else
    echo "Unsupported env"
    exit 1
fi


