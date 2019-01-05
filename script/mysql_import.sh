#!/usr/bin/env bash
set -ex

file=$1
pw=$2
if [ -z $file ] || [ -z $pw ]; then
  echo "Usage: $0 [FILE_PATH] [PASSWORD]"
  exit 1
fi


dir=$(mktemp -d)
split -l 100000 -d $file "$dir/announcement.csv."

for f in $dir/*; do
  cp $f "$dir/announcement.csv"
  mysqlimport --local -h 192.168.1.204 -P 3307 -u root -p$pw --fields-terminated-by=, \
  --columns="id,title,content,affair_id,thumb_content,state,alliance_id,tags,modify_time,plate_type" test_dev "$dir/announcement.csv"
done

