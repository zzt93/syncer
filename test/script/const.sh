#!/usr/bin/env bash


declare -A db2table
# tables in test.sql
db2table["test"]="news correctness types"
# tables in simple.sql
db2table["simple"]="simple_type"
db2table["discard"]="toDiscard"
db2table["copy"]="to_copy"

defaultDBs="test simple"
allDBs=""

function initAllDBs() {
    for f in generator/*.sql; do
        filename=`basename ${f}`
        dbName=${filename%".sql"}
        allDBs="$allDBs $dbName"
    done
}

initAllDBs