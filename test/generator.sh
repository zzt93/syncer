#!/usr/bin/env bash

num=$1
env=$2

cd generator
docker build . -f DataGenerator.Dockerfile -t generator:test
cd ..

docker run -v data:/data generator:test /mysql_multi.sql $1
docker run -v data:/data generator:test /mysql_simple.sql $1

if [[ $env = "mysql" ]]; then
    tmp=`mktemp`
    echo "create database test; use test;" > ${tmp}
    cat generator/mysql_multi.sql >> ${tmp}
    cat generator/mysql_simple.sql >> ${tmp}
    export mysql_init=${tmp}

    docker-compose -f mysql.yml up -d
    docker run -v data/:/data/ --rm mysql:5.7.15 mysqlimport --fields-terminated-by=, --verbose --local -u root -proot -P43306 test /data/*.csv

elif [[ $env = "drds" ]]; then
    for (( i = 0; i < 3; ++i )); do
        tmp=`mktemp`
        echo "create database test_$i; use test_$i;" > $tmp
        cat generator/mysql_multi.sql >> ${tmp}
        cat generator/mysql_simple.sql >> ${tmp}
        export mysql_init_${i}=${tmp}
    done

    docker-compose -f drds.yml up -d

    for (( i = 0; i < 3; ++i )); do
        port=$((43306+$i))
        docker run -v data/:/data/ --rm mysql:5.7.15 mysqlimport --fields-terminated-by=, --verbose --local -u root -proot -P$port test_$i /data/*.csv
    done
else
    echo "Unsupported env"
    exit 1
fi


