#! /bin/bash

SCRIPT=$(readlink -f $0)
SCRIPTS_HOME="$(dirname ${SCRIPT})"
DOCKER_PORTS=$1
CONTAINER_NAME=$2
ATSD_LOGIN=$3
ATSD_PASSWORD=$4

HOST=127.0.0.1

docker run -d --name=${CONTAINER_NAME} ${DOCKER_PORTS} -e axiname="$ATSD_LOGIN" -e axipass="$ATSD_PASSWORD" -e timezone="Europe/Berlin" axibase/atsd:api_test

TCP_PORT=$(docker port ${CONTAINER_NAME} 8081| cut -d ":" -f2)
HTTPS_PORT=$(docker port ${CONTAINER_NAME} 8443| cut -d ":" -f2)

echo "TCP port is ${TCP_PORT}"
echo "HTTPS port is ${HTTPS_PORT}"

while [[ $(curl --user $ATSD_LOGIN:$ATSD_PASSWORD --write-out %{http_code} --silent --output /dev/null https://$HOST:$HTTPS_PORT/version) != 200 ]];
do
    echo "waiting to start server ...";
    sleep 3;
done

echo "ATSD is ready"

echo "==========================================="
echo "Start data preparation at $(date +%Y-%m-%d\ %H-%M-%S)"
echo "==========================================="

echo "Inserting m_small with 100 records..."
nc $HOST $TCP_PORT < $SCRIPTS_HOME/docker/m_small
echo "m_small created"


echo "Inserting m_large with 500000 records..."
nc $HOST $TCP_PORT < $SCRIPTS_HOME/docker/m_large
echo "m_large created"

echo "==========================================="
echo "Finished data preparation at $(date +%Y-%m-%d\ %H-%M-%S)"
echo "==========================================="
