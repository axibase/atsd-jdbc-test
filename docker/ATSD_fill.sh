#! /bin/bash

function logger {
    echo " * [FILL] $1" 
}

logger "==========================================="
logger "Start to fill at $(date +%Y-%m-%d\ %H-%M-%S)"
logger "==========================================="

logger "Determining host"
HOST_NAME=$(cat /proc/sys/kernel/hostname)


logger "Creating ATSD instance..."
/opt/atsd/bin/entrypoint.sh &

logger "Waiting for ATSD start..."
ports=(8081 8082 8088 8443)
for item in ${ports[*]}
do
    ans=""
	while true; 
	do 
		if [ -n "$ans" ]; then
		sleep 6
  		logger "Port $item is ok"
  		break
  		fi
	ans=$(netstat -tuln 2>/dev/null | grep ${item})
	done
done
logger "ATSD is ready"


logger "Creating admin user=$axiname..."
curl --data "userBean.username=$axiname&userBean.password=$axipass&repeatPassword=$axipass" http://${HOST_NAME}:8088/login
logger "User created"


logger "Filling m_small, 100 records..."
nc ${HOST_NAME} 8081 < m_small
logger "m_small created"


logger "Filling m_large, 500000 records..."
nc ${HOST_NAME} 8081 < m_large
logger "m_large created"


logger "Safety stopping ATSD..."
/opt/atsd/bin/atsd-all.sh stop

logger "Wait 30 seconds..."
sleep 30

logger "==========================================="
logger "Finished to fill at $(date +%Y-%m-%d\ %H-%M-%S)"
logger "==========================================="