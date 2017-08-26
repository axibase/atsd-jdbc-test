#!/usr/bin/env bash

AXINAME=$1
AXIPASS=$2
HTTPS_PORT=$3
TCP_PORT=$4
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

logger "Creating admin user=$AXINAME..."
curl --data "userBean.username=$AXINAME&userBean.password=$AXIPASS&repeatPassword=$AXIPASS" https://$HOST:$HTTPS_PORT/login --insecure
curl -u $AXINAME:$AXIPASS --data "options%5B0%5D.key=last.insert.write.period.seconds&options%5B0%5D.value=0&apply=Save" https://$HOST:$HTTPS_PORT/admin/serverproperties --insecure
logger "User created"

logger "Inserting m_small with 100 records..."
nc $HOST $TCP_PORT < $SCRIPTS_HOME/m_small
logger "m_small created"


logger "Inserting m_large with 500000 records..."
nc $HOST $TCP_PORT < $SCRIPTS_HOME/m_large
logger "m_large created"

logger "==========================================="
logger "Finished data preparation at $(date +%Y-%m-%d\ %H-%M-%S)"
logger "==========================================="
