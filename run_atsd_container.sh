#! /bin/bash

SCRIPT=$(readlink -f $0)
SCRIPTS_HOME="$(dirname ${SCRIPT})"
DOCKER_PORTS=$1
CONTAINER_NAME=$2
ATSD_LOGIN=$3
ATSD_PASSWORD=$4

docker run -d --name="atsd-jdbc-test" ${DOCKER_PORTS} -e axiname="$ATSD_LOGIN" -e axipass="$ATSD_PASSWORD" -e timezone="Europe/Berlin" axibase/atsd:api_test

TCP_PORT=$(docker port ${CONTAINER_NAME} 8081| cut -d ":" -f2)
HTTPS_PORT=$(docker port ${CONTAINER_NAME} 8443| cut -d ":" -f2)

echo "TCP port is ${TCP_PORT}"
echo "HTTPS port is ${HTTPS_PORT}"

bash ${SCRIPTS_HOME}/docker/ATSD_fill.sh ${HTTPS_PORT} ${TCP_PORT}