#! /bin/bash

if [ $(echo "${1}"|sed 's/ -p/\n-p/g'|grep -c "p") -ne 2 ] && [ "${1}" != "--publish-all" ];then
    echo "You must specify two ports at least or use --publish-all"
else
    ./run_atsd_container.sh ${1}

    if [ -n "${2}" ]; then
	    echo "TEST_OPTIONS are ${2}"
    else
        echo "TEST_OPTIONS are empty"
    fi

    mvn clean test -B ${2}
    docker ps -a | grep "jdbc_test_" | awk '{print $1}' | xargs docker rm -vf
    docker rmi atsd:jdbc-test
fi