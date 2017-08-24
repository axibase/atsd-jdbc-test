#! /bin/bash

SCRIPT=$(readlink -f $0)
SCRIPTS_HOME="$(dirname ${SCRIPT})"
DOCKERFILE="${SCRIPTS_HOME}/docker/"
#HOST_NAME=$(cat /proc/sys/kernel/hostname)
DOCKER_PORTS=${1}
CONTAINER_NAME=${2}

docker build --no-cache=true -t atsd:${CONTAINER_NAME} ${DOCKERFILE}

docker run -d --name=${CONTAINER_NAME} ${DOCKER_PORTS} atsd:${CONTAINER_NAME}

TCP_PORT=$(docker port ${CONTAINER_NAME} 8081| cut -d ":" -f2)
echo "TCP port is ${TCP_PORT}"

echo -n "Waiting for ATSD start"
ans=""
while true;
	do
		if [ -n "$ans" ]; then
			echo -e "\nPing is ok"
  		    break
  		fi
    sleep 3
	ans=$(echo ping|nc localhost ${TCP_PORT})
	echo -n "."
done
