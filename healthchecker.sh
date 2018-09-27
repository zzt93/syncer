#!/bin/bash
#set -x

temp_file=$(mktemp)
wget -O $temp_file http://127.0.0.1:10000/health
status=`egrep -o 'overall":{"status":"[^"]+' $temp_file | awk -F '"' '{print $NF}' `
if [ "$status" = "GREEN" ] ; then
    exit 0
else
  exit 1
fi