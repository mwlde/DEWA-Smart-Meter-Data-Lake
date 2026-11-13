#!/bin/bash
#copies the generated data into the hdfs raw zone, replacing what was there.
#run inside the hadoop container, from the folder that contains data/
#  bash scripts/load_raw_zone.sh 2>/dev/null
set -eE
trap 'echo; echo "FAILED in load_raw_zone.sh at line $LINENO: $BASH_COMMAND"; echo "Full details: run it again without 2>/dev/null"' ERR

BASE=/user/hadoop/dewa/raw

if [ ! -f data/meters.csv ] || ! ls -d data/readings/dt=* > /dev/null 2>&1; then
  echo "FAILED: no generated data in $(pwd)/data"
  echo "What to do: cd /home/hadoop/dewa, then python3 data_generator/generate.py"
  exit 1
fi
if ! hdfs dfsadmin -report > /dev/null 2>&1; then
  echo "FAILED: HDFS is not running (jps should list NameNode and DataNode)"
  echo "What to do: on your computer, docker restart hadoop"
  exit 1
fi

#clear the old raw zone first: regenerating with fewer days would otherwise leave
#old dt=... folders behind, and hive would still read them
hdfs dfs -rm -r -f -skipTrash $BASE > /dev/null
hdfs dfs -mkdir -p $BASE/meters $BASE/readings $BASE/ground_truth

hdfs dfs -put -f data/meters.csv $BASE/meters/
hdfs dfs -put -f data/ground_truth.csv $BASE/ground_truth/
#empty premises (normal behaviour, used by evaluation/evaluate.py); older datasets have none
if [ -f data/vacancies.csv ]; then
  hdfs dfs -mkdir -p $BASE/vacancies
  hdfs dfs -put -f data/vacancies.csv $BASE/vacancies/
fi
for day in data/readings/dt=*; do
  hdfs dfs -put -f "$day" $BASE/readings/
done

echo
echo "Raw zone contents:"
hdfs dfs -ls -R /user/hadoop/dewa/raw | head -20
hdfs dfs -du -s -h /user/hadoop/dewa/raw
