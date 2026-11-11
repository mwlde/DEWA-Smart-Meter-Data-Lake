#!/bin/bash
# Copies the generated data into the HDFS raw zone.
# Run INSIDE the Hadoop container, from the folder that contains data/
#   bash scripts/load_raw_zone.sh
set -e

BASE=/user/hadoop/dewa/raw

hdfs dfs -mkdir -p $BASE/meters $BASE/readings $BASE/ground_truth

hdfs dfs -put -f data/meters.csv $BASE/meters/
hdfs dfs -put -f data/ground_truth.csv $BASE/ground_truth/
for day in data/readings/dt=*; do
  hdfs dfs -put -f "$day" $BASE/readings/
done

echo
echo "Raw zone contents:"
hdfs dfs -ls -R /user/hadoop/dewa/raw | head -20
hdfs dfs -du -s -h /user/hadoop/dewa/raw
