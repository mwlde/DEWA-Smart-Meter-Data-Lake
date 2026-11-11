#!/bin/bash
#runs every time the container starts: starts hdfs, yarn and the job history
#server, then keeps the container alive. hdfs is formatted on the first start only.

DATA=/home/hadoop/hadoop-data

#stale process-id files from the last run make hadoop think a daemon is still running
rm -f /tmp/hadoop-*.pid /tmp/yarn-*.pid /tmp/mapred-*.pid

if [ ! -f "$DATA/namenode/current/VERSION" ]; then
  echo "First start: formatting HDFS"
  hdfs namenode -format -nonInteractive -force > /home/hadoop/format.log 2>&1
fi

hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
mapred --daemon start historyserver

hdfs dfsadmin -safemode wait > /dev/null 2>&1
hdfs dfs -mkdir -p /user/hadoop /tmp > /dev/null 2>&1
echo "Hadoop is up"

tail -f /dev/null
