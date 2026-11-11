#!/bin/bash
#quick check that every project tool is installed and hadoop is up.
#run inside the container:  bash /home/hadoop/dewa/scripts/check_tools.sh
source ~/.bashrc

ok() { printf "  %-8s %s\n" "$1" "$2"; }

echo "Java:"
java -version 2>&1 | head -1

echo
echo "Tools:"
for t in hdfs hive spark-submit kafka-topics.sh hbase; do
  if command -v $t >/dev/null; then ok "OK" "$t"; else ok "MISSING" "$t"; fi
done

echo
echo "Hadoop daemons (need NameNode, DataNode, ResourceManager, NodeManager):"
jps | grep -v Jps

echo
echo "HDFS:"
hdfs dfsadmin -report 2>/dev/null | grep "Live datanodes" || echo "  HDFS not reachable"
