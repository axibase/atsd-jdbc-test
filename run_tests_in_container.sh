#! /bin/bash

DOCKER_PORTS=$1
TEST_OPTIONS=$2

if [ $(echo "${DOCKER_PORTS}"|sed 's/ -p/\n-p/g'|grep -c "p") -ne 2 ] && [ "${DOCKER_PORTS}" != "--publish-all" ];then
    echo "You must specify two ports at least or use --publish-all"
else
    CONTAINER_NAME=jdbc_test_$(date +%s%N | cut -b1-13)
    ./run_atsd_container.sh "${DOCKER_PORTS}" "${CONTAINER_NAME}" axibase axibase

    if [ -n "${TEST_OPTIONS}" ]; then
	    echo "TEST_OPTIONS are ${TEST_OPTIONS}"
    else
        echo "TEST_OPTIONS are empty"
    fi

    HTTPS_PORT=$(docker port ${CONTAINER_NAME} 8443| cut -d ":" -f2)

    echo "HTTPS port is ${HTTPS_PORT}"

    mvn clean test -B ${TEST_OPTIONS} -Daxibase.tsd.driver.jdbc.url=localhost:${HTTPS_PORT}
    docker rm -vf ${CONTAINER_NAME}
fi
