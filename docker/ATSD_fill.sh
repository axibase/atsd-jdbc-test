#!/usr/bin/env bash

HTTPS_PORT=$1
TCP_PORT=$2
SCRIPTS_HOME=$(dirname $0)
HOST="127.0.0.1"

function logger {
    echo " * [FILL] $1" 
}

logger "==========================================="
logger "Start data preparation at $(date +%Y-%m-%d\ %H-%M-%S)"
logger "==========================================="

logger "Waiting for ATSD start..."
ping_ok=""
while [ -z $ping_ok ];
do
    sleep 3
	ping_ok=$(echo ping|nc $HOST $TCP_PORT)
done
logger "ATSD is ready"

logger "Inserting m_small with 100 records..."
nc $HOST $TCP_PORT < $SCRIPTS_HOME/m_small
logger "m_small created"


logger "Inserting m_large with 500000 records..."
nc $HOST $TCP_PORT < $SCRIPTS_HOME/m_large
logger "m_large created"

logger "==========================================="
logger "Finished data preparation at $(date +%Y-%m-%d\ %H-%M-%S)"
logger "==========================================="
