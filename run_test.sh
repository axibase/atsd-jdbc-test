#! /bin/bash

SCRIPT=$(readlink -f $0)
SCRIPTS_HOME="`dirname $SCRIPT`"
DOCKERFILE="${SCRIPTS_HOME}/src/test/resources/Dockerfile/"
PROPS="${SCRIPTS_HOME}/src/test/resources/dev.properties"

IP=127.0.0.1
array=()

function scanner
{
first_port=1025
last_port=65535
j=0
for ((port=$first_port; port<=$last_port; port++))
        do
		(echo >/dev/tcp/$IP/$port)> /dev/null 2>&1
		if [ $? -eq 1 ]; then
    		array[j]=$port
		j="`echo $(( $j  + 1 ))`"
			if [ $j -eq 2 ]; then
			break;
			fi
		fi
        done
}

docker build --no-cache=true -t atsd:jdbc-test ${DOCKERFILE}

#Looking for free ports
scanner

#Customize src/test/resources/dev.properties, the first five properties are mandatory
echo "	axibase.tsd.driver.jdbc.url=localhost:${array[0]}
		axibase.tsd.driver.jdbc.username=axibase
		axibase.tsd.driver.jdbc.password=axibase
		axibase.tsd.driver.jdbc.metric.tiny.count=100
		axibase.tsd.driver.jdbc.metric.tiny=m_small

		axibase.tsd.driver.jdbc.metric.large=m_large
		axibase.tsd.driver.jdbc.strategy=stream
		axibase.tsd.driver.jdbc.metric.wrong=hgashjfgajhfg

		#axibase.tsd.driver.jdbc.metric.small=
		#axibase.tsd.driver.jdbc.metric.medium=jvm_memory_used
		#axibase.tsd.driver.jdbc.metric.huge=
		#axibase.tsd.driver.jdbc.metric.jumbo=
		#axibase.tsd.driver.jdbc.metric.concurrent=" > ${PROPS}

docker run -d --name=jdbc_test --publish ${array[0]}:8443 --publish ${array[1]}:8081 atsd:jdbc-test

echo "Waiting for ATSD starts..."

ans=""
while true; 
	do 
		if [[ "$ans" != "" ]]; then
			logger "Ping is ok"
  		break
  		fi
	ans="`echo ping|nc ${IP} ${array[1]} 2>/dev/null`"
done

mvn test

docker rm -fv jdbc_test
docker rmi atsd:jdbc-test


#beforre_install:
#  - sudo apt-get update -qq
#  - sudo apt-get install -qq maven
#install: ./run_test.sh
#script:
#  - mvn test -B
#after_script:
#  - docker rm -fv jdbc_test
#  - docker rmi atsd:jdbc-test