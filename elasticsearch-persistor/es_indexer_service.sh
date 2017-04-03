#!/bin/bash

COMMAND="python3 elasticsearch_indexer.py"
SERVICE_NAME="Elasticsearch Indexer"
PID_FOLDER=pids
PID_FILE=$PID_FOLDER/elasticsearch_indexer.pid
SERVER_COMMAND="./es_queuer_server.sh"
SERVER_NAME="ES Server"
CURRENT_SERV_PID=0
if [ -e "$PID_FILE" ];
   then
     CURRENT_SERV_PID=`cat $PID_FILE`
     if ! (ps -ef | grep $CURRENT_SERV_PID | grep -v grep 1>/dev/null);
       then CURRENT_SERV_PID=0;
     fi
fi

ES_ARGS=$1
if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    ES_ARGS=""
  else
    ES_ARGS="'$ES_ARGS'"
fi

if [ $CURRENT_SERV_PID -eq 0 ];
  then
       echo "Starting redis"
       echo "/etc/init.d/redis-server start"
       eval "/etc/init.d/redis-server start"
       echo "Starting $SERVER_NAME"
       echo "$SERVER_COMMAND start &"
       eval "$SERVER_COMMAND start &"
       echo "Starting $SERVICE_NAME"
       echo "$COMMAND $ES_ARGS"
       eval "$COMMAND $ES_ARGS"
  else
       echo "Service $SERVICE_NAME already running with pid: $CURRENT_SERV_PID ";
fi

