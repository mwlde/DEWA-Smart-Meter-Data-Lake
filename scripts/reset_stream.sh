#!/bin/bash
#clears everything the streaming run produced, so the next run starts fresh:
#the kafka topics, and the alerts, landing, checkpoint and evaluation folders in hdfs.
#the raw zone and the baseline are kept.
#run inside the container:  bash scripts/reset_stream.sh
BASE=/user/hadoop/dewa
BOOT=localhost:9092

hdfs dfs -rm -r -f $BASE/checkpoints $BASE/alerts $BASE/landing $BASE/evaluation 2>/dev/null

for t in meter-readings alerts; do
  kafka-topics.sh --bootstrap-server $BOOT --delete --topic $t 2>/dev/null || true
done
sleep 5
bash "$(dirname "$0")/start_kafka.sh"
echo "Reset done."
