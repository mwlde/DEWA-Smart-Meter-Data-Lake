#hadoop 3.3.6 (hdfs + yarn) in pseudo-distributed mode, ubuntu 22.04, java 8.
#builds on windows and intel macs (amd64) and on apple silicon (arm64).
#hive, spark, kafka and hbase are added afterwards by scripts/setup_container.sh.
#
#build (from the repo folder):  docker build -t dewa-hadoop .
#see SETUP.md for the full steps.

FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-8-jdk-headless curl ca-certificates python3 procps nano less \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(ls -d /usr/lib/jvm/java-8-openjdk-*)" /usr/lib/jvm/java-8 \
    && useradd -m -s /bin/bash hadoop

ENV JAVA_HOME=/usr/lib/jvm/java-8 \
    HADOOP_HOME=/home/hadoop/hadoop \
    HADOOP_CONF_DIR=/home/hadoop/hadoop/etc/hadoop
ENV PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

USER hadoop
WORKDIR /home/hadoop

ARG HADOOP_VERSION=3.3.6
RUN curl -fL --retry 3 -o hadoop.tgz \
        https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && mkdir hadoop && tar -xzf hadoop.tgz -C hadoop --strip-components=1 && rm hadoop.tgz \
    && echo "export JAVA_HOME=/usr/lib/jvm/java-8" >> hadoop/etc/hadoop/hadoop-env.sh

COPY --chown=hadoop:hadoop docker/hadoop-conf/ /home/hadoop/hadoop/etc/hadoop/
COPY --chown=hadoop:hadoop docker/start.sh /home/hadoop/start.sh
RUN chmod +x /home/hadoop/start.sh && mkdir -p /home/hadoop/dewa

#namenode ui, yarn ui, job history ui
EXPOSE 9870 8088 19888

CMD ["/home/hadoop/start.sh"]
