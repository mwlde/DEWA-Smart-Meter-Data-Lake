#!/bin/bash
#clears everything the streaming run produced, so the next run starts fresh:
#the kafka topics, the alerts, landing, checkpoint and evaluation folders in hdfs,
#and (if hbase is running) the rows in the hbase tables.
#the raw zone, the baseline and the cleansed and curated zones are kept.
#run inside the container:  bash scripts/reset_stream.sh
BASE=/user/hadoop/dewa
BOOT=localhost:9092

#a detection job left running by an earlier run (if the script that started it
#stopped on an error) would fight the new one over the checkpoints: stop it first
if pgrep -f "streaming/detect_stream.py" > /dev/null; then
  echo "Stopping a detection job still running from an earlier run..."
  pkill -f "streaming/detect_stream.py" || true
  sleep 5
fi

echo "Clearing streaming output in HDFS..."
hdfs dfs -rm -r -f $BASE/checkpoints $BASE/alerts $BASE/landing $BASE/evaluation > /dev/null 2>&1

echo "Deleting the Kafka topics..."
for t in meter-readings alerts; do
  kafka-topics.sh --bootstrap-server $BOOT --delete --topic $t > /dev/null 2>&1 || true
done

if jps | grep -q " HMaster$"; then
  echo "Emptying the HBase tables..."
  printf "truncate 'meter_readings'\ntruncate 'meter_alerts'\n" | hbase shell -n > /dev/null 2>&1 \
    || echo "  (could not empty the HBase tables; run bash scripts/start_hbase.sh and try again)"
fi

sleep 5
bash "$(dirname "$0")/start_kafka.sh" || exit 1
echo "Reset done."
