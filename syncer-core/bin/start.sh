#!/usr/bin/env bash
producer=$1
consumer=$2

if [ -z "$producer" ]; then
    producer="$SYNCER_PRODUCER"
    if [ -z "$producer" ]; then
        echo "No producer config found"
        exit 1
    fi
fi

if [ -z "$consumer" ]; then
    consumer="$SYNCER_CONSUMER"
    if [ -z "$consumer" ]; then
        echo "No consumer config found"
        exit 1
    fi
fi

syncer_home="$SYNCER_HOME"
if [ -z "$syncer_home" ]; then
    syncer_home="/"
fi


if [ -x "$JAVA_HOME/bin/java" ]; then
    JAVA="$JAVA_HOME/bin/java"
else
    JAVA=`which java`
fi

if [ ! -x "$JAVA" ]; then
    echo "Could not find any executable java binary. Please install java in your PATH or set JAVA_HOME"
    exit 1
fi

#HOSTNAME=`hostname | cut -d. -f1`
export HOSTNAME

$JAVA -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=40100 -server -XX:+UseG1GC -jar $syncer_home/syncer.jar --port=40000 --producerConfig=$producer --consumerConfig=$consumer $EXTRA_OPT
