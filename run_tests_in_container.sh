#! /bin/bash

DOCKER_PORTS=${1}
TEST_OPTIONS=${2}

if [ $(echo "${DOCKER_PORTS}"|sed 's/ -p/\n-p/g'|grep -c "p") -ne 2 ] && [ "${DOCKER_PORTS}" != "--publish-all" ];then
    echo "You must specify two ports at least or use --publish-all"
else
    timestamp="$(date +%s%N | cut -b1-13)"
    CONTAINER_NAME=jdbc_test_${timestamp}
    ./run_atsd_container.sh "${DOCKER_PORTS}" "${CONTAINER_NAME}"

    if [ -n "${TEST_OPTIONS}" ]; then
	    echo "TEST_OPTIONS are ${TEST_OPTIONS}"
    else
        echo "TEST_OPTIONS are empty"
    fi

    mvn clean test -B ${TEST_OPTIONS}
    docker rm -vf ${CONTAINER_NAME}
    docker rmi atsd:${CONTAINER_NAME}
fi