#!/bin/bash

SERVER_COMMAND="./es_queuer_server.sh"
SERVER_NAME="ES Server"
INDEXER_COMMAND="./es_indexer.sh"
INDEXER_NAME="ES Indexer"
case $1 in
     start)
         echo "Starting redis"
         echo "/etc/init.d/redis-server start"
         eval "/etc/init.d/redis-server start"         
         echo "Starting $SERVER_NAME"
         echo "$SERVER_COMMAND start &"
         eval "$SERVER_COMMAND start &"
         echo "Starting $INDEXER_NAME"
         echo "$INDEXER_COMMAND start &"
         eval "$INDEXER_COMMAND start &"
         
     ;;
     stop)
         echo "Stopping redis"
         echo "/etc/init.d/redis-server stop"
         eval "/etc/init.d/redis-server stop"         
         echo "Stopping $SERVER_NAME"
         echo "$SERVER_COMMAND stop "
         eval "$SERVER_COMMAND stop "
         echo "Stopping $INDEXER_NAME"
         echo "$INDEXER_COMMAND stop "
         eval "$INDEXER_COMMAND stop "
     ;;
     status)
         echo "Status of redis"
         echo "/etc/init.d/redis-server status"
         eval "/etc/init.d/redis-server status"         
         echo "Status of $SERVER_NAME"
         echo "$SERVER_COMMAND status "
         eval "$SERVER_COMMAND status "
         echo "Status of $INDEXER_NAME"
         echo "$INDEXER_COMMAND status "
         eval "$INDEXER_COMMAND status "
     ;;
     *)
       echo "usage: start {start|stop}" ;;
esac

