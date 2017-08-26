#! /bin/bash

SCRIPT=$(readlink -f $0)
SCRIPTS_HOME="$(dirname ${SCRIPT})"
DOCKER_PORTS=$1
CONTAINER_NAME=$2

docker run -d --name=${CONTAINER_NAME} ${DOCKER_PORTS} axibase/atsd

TCP_PORT=$(docker port ${CONTAINER_NAME} 8081| cut -d ":" -f2)
HTTPS_PORT=$(docker port ${CONTAINER_NAME} 8443| cut -d ":" -f2)

echo "TCP port is ${TCP_PORT}"
echo "HTTPS port is ${HTTPS_PORT}"

bash ${SCRIPTS_HOME}/docker/ATSD_fill.sh axibase axibase ${HTTPS_PORT} ${TCP_PORT}