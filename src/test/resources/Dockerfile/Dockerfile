FROM raipc/java-parent:j7u1404
MAINTAINER ATSD Developers <dev-atsd@axibase.com>
ENV LANG en_US.UTF-8

ENV axiname=axibase
ENV axipass=axibase

#metadata
LABEL com.axibase.vendor="Axibase Corporation" \
  com.axibase.product="Axibase Time Series Database" \
  com.axibase.code="ATSD"
#configure system
RUN locale-gen en_US.UTF-8 \
  && adduser --disabled-password --quiet --gecos "" axibase;

#apt-get jobs
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com --recv-keys 26AEE425A57967CFB323846008796A6514F3CB79 \
  && echo "deb [arch=amd64] http://axibase.com/public/repository/deb/ ./" >> /etc/apt/sources.list \
  && apt-get update && apt-get install --no-install-recommends -y atsd && rm -rf /var/lib/apt/lists/*; 

#scripts which insert into ATSD required data
ADD ATSD_fill.sh /
ADD m_small /
ADD m_large /

USER axibase

#creating m_small (100 samples) and m_large (500 000 samples) metrics
RUN ["/bin/bash", "./ATSD_fill.sh"]

#set hbase distributed mode false
RUN sed -i '/.*hbase.cluster.distributed.*/{n;s/.*/   <value>false<\/value>/}' /opt/atsd/hbase/conf/hbase-site.xml

#jmx, atsd(tcp), atsd(udp), pickle, http, https
EXPOSE 1099 8081 8082/udp 8084 8088 8443

VOLUME ["/opt/atsd"]

ENTRYPOINT ["/bin/bash","/opt/atsd/bin/entrypoint.sh"]
