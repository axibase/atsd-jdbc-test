#! /bin/bash

SCRIPT=$(readlink -f $0)
SCRIPTS_HOME="`dirname $SCRIPT`"
DOCKERFILE="${SCRIPTS_HOME}/docker/"
PROPS="${SCRIPTS_HOME}/src/test/resources/dev.properties"

array=()

function scanner
{
first_port=1025
last_port=65535
j=0
for ((port=$first_port; port<=$last_port; port++))
        do
		(echo >/dev/tcp/127.0.0.1/$port)> /dev/null 2>&1
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

#Looking for free ports if there is no parameters
if [ -z "$1" ] || [ -z "$2" ]; then
	scanner
else
    array[0]="$1"
    array[1]="$2"
fi

echo "HTTPS port is ${array[0]}, TCP port is ${array[1]}"

#Customize src/test/resources/dev.properties, the first five properties are mandatory
echo "axibase.tsd.driver.jdbc.url=localhost:${array[0]}
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

docker run -d --name=jdbc_test --publish ${array[0]}:8443 --publish ${array[1]}:8081 atsd:jdbc-test

echo "Waiting for ATSD starts..."

ans=""
while true; 
	do 
		if [ -n "$ans" ]; then
			logger "Ping is ok"
  		break
  		fi
	ans="`echo ping|nc 127.0.0.1 ${array[1]} 2>/dev/null`"
done

#docker rm -fv jdbc_test
#docker rmi atsd:jdbc-test