#! /bin/bash

SCRIPT=$(readlink -f $0)
SCRIPTS_HOME="$(dirname ${SCRIPT})"
DOCKERFILE="${SCRIPTS_HOME}/docker/"
PROPS="${SCRIPTS_HOME}/src/test/resources/dev.properties"
HOST_NAME=$(cat /proc/sys/kernel/hostname)
DOCKER_PORTS=${1}
CONTAINER_NAME=${2}

docker build --no-cache=true -t atsd:${CONTAINER_NAME} ${DOCKERFILE}

docker run -d --name=${CONTAINER_NAME} ${DOCKER_PORTS} atsd:${CONTAINER_NAME}

TCP_PORT=$(docker port ${CONTAINER_NAME} 8081| cut -d ":" -f2)
HTTPS_PORT=$(docker port ${CONTAINER_NAME} 8443| cut -d ":" -f2)
   
echo "HTTPS port is ${HTTPS_PORT}, TCP port is ${TCP_PORT}"

#Customization of src/test/resources/dev.properties, the first five properties are mandatory
echo "axibase.tsd.driver.jdbc.url=localhost:${HTTPS_PORT}
	  axibase.tsd.driver.jdbc.username=axibase
	  axibase.tsd.driver.jdbc.password=axibase
	  axibase.tsd.driver.jdbc.metric.tiny.count=100
	  axibase.tsd.driver.jdbc.metric.tiny=m_small

	  axibase.tsd.driver.jdbc.metric.large=m_large
	  axibase.tsd.driver.jdbc.strategy=stream
	  axibase.tsd.driver.jdbc.metric.wrong=hgashjfgajhfg

	  #axibase.tsd.driver.jdbc.metric.small=
	  #axibase.tsd.driver.jdbc.metric.medium=
	  #axibase.tsd.driver.jdbc.metric.huge=
	  #axibase.tsd.driver.jdbc.metric.jumbo=
	  #axibase.tsd.driver.jdbc.metric.concurrent=" > ${PROPS}

echo -n "Waiting for ATSD start"
ans=""
while true;
	do
		if [ -n "$ans" ]; then
			echo -e "\nPing is ok"
  		    break
  		fi
    sleep 3
	ans=$(echo ping|nc ${HOST_NAME} ${TCP_PORT})
	echo -n "."
done
