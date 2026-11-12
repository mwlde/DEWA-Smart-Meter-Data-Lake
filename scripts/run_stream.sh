#!/bin/bash
#one full streaming run in one terminal, start to finish:
#reset, kafka, hbase, baseline (if missing), the detection job in the background,
#the replay in the foreground, then the evaluation.
#run inside the container, from /home/hadoop/dewa:
#  bash scripts/run_stream.sh              (with hbase)
#  bash scripts/run_stream.sh --no-hbase   (without hbase)
#to watch the alerts live, open a second terminal and run:
#  tail -f logs/stream.log
set -eE -o pipefail
trap 'echo; echo "FAILED in run_stream.sh at line $LINENO: $BASH_COMMAND"' ERR
cd "$(dirname "$0")/.."

HBASE_FLAG="--hbase"
[ "$1" = "--no-hbase" ] && HBASE_FLAG=""
KAFKA_PKG=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6
LOG=logs/stream.log
mkdir -p logs

say() { echo; echo "=== $1"; }

say "1/6 resetting the last run"
bash scripts/reset_stream.sh

if [ -n "$HBASE_FLAG" ]; then
  say "2/6 starting hbase"
  bash scripts/start_hbase.sh
else
  say "2/6 hbase skipped (--no-hbase)"
fi

say "3/6 checking the baseline"
#rebuild it if it is missing or older than the raw zone (new data needs a new baseline)
RAW_T=$(hdfs dfs -stat "%Y" /user/hadoop/dewa/raw/readings 2>/dev/null || echo 0)
BASE_T=$(hdfs dfs -stat "%Y" /user/hadoop/dewa/curated/meter_baseline 2>/dev/null || echo 0)
if [ "$BASE_T" -gt "$RAW_T" ]; then
  echo "baseline is up to date"
else
  echo "building the baseline from the raw zone"
  spark-submit streaming/baseline.py 2>/dev/null
fi

say "4/6 starting the detection job (output in $LOG)"
spark-submit --packages $KAFKA_PKG streaming/detect_stream.py --kafka-alerts $HBASE_FLAG > "$LOG" 2>/dev/null &
JOB=$!
for i in $(seq 1 150); do
  grep -q "waiting for readings" "$LOG" 2>/dev/null && break
  if ! kill -0 $JOB 2>/dev/null; then
    echo "FAILED: the detection job stopped before it was ready. End of $LOG:"
    tail -15 "$LOG"
    exit 1
  fi
  sleep 2
done
if ! grep -q "waiting for readings" "$LOG"; then
  echo "FAILED: the detection job was not ready after 5 minutes. End of $LOG:"
  tail -15 "$LOG"
  kill $JOB 2>/dev/null || true
  exit 1
fi
echo "detection job ready"

say "5/6 replaying the readings into kafka (about 10 mins for 6 days)"
python3 streaming/replay.py --interval 1 \
  | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic meter-readings \
      --property parse.key=true --property key.separator='|' > /dev/null
echo "waiting for the detection job to finish (it stops 30 secs after the last reading)..."
if ! wait $JOB; then
  echo "FAILED: the detection job stopped with an error. End of $LOG:"
  tail -15 "$LOG"
  exit 1
fi
echo "detection job finished: $(grep -c 'alert(s)$' "$LOG" || true) alert batches, full output in $LOG"

say "6/6 evaluating"
spark-submit evaluation/evaluate.py 2>/dev/null

echo
echo "Run finished."
[ -n "$HBASE_FLAG" ] && echo "HBase queries: bash scripts/hbase_queries.sh"
exit 0
