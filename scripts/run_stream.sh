#!/bin/bash
#one full streaming run in one terminal, start to finish:
#reset, kafka, hbase, baseline (if missing), the detection job in the background,
#the replay in the foreground, then the evaluation.
#run inside the container, from /home/hadoop/dewa:
#  bash scripts/run_stream.sh              (with hbase)
#  bash scripts/run_stream.sh --no-hbase   (without hbase)
#options (any order):
#  --interval N   real secs per 15-min interval in the replay (default 1)
#  --days N       replay only the first N live days (default all)
#  --no-late      send every reading on time (default: 1% arrive 15 or 30 mins late)
#  --tag NAME     also save the evaluation summary to hdfs .../dewa/speed_test/NAME
#to watch the alerts live, open a second terminal and run:
#  tail -f logs/stream.log
set -eE -o pipefail
JOB=""
#on any failure: say where, and stop the detection job so it is not left running
trap 'echo; echo "FAILED in run_stream.sh at line $LINENO: $BASH_COMMAND"; [ -n "$JOB" ] && kill $JOB 2>/dev/null; echo "Logs: logs/stream.log (alerts), logs/stream_spark.log (spark errors)"' ERR
cd "$(dirname "$0")/.."

HBASE_FLAG="--hbase"
INTERVAL=1
REPLAY_OPTS="--late-share 0.01 --late-max 2"
EVAL_OPTS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --no-hbase) HBASE_FLAG="" ;;
    --interval) INTERVAL=$2; shift ;;
    --days) REPLAY_OPTS="$REPLAY_OPTS --days $2"; shift ;;
    --no-late) REPLAY_OPTS="${REPLAY_OPTS/--late-share 0.01/--late-share 0}" ;;
    --tag) EVAL_OPTS="--save-to /user/hadoop/dewa/speed_test/$2"; shift ;;
    *) echo "FAILED: unknown option $1"; exit 1 ;;
  esac
  shift
done
KAFKA_PKG=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6
LOG=logs/stream.log
SPARK_LOG=logs/stream_spark.log
mkdir -p logs

say() { echo; echo "=== $1"; }

#hbase first: the reset only empties the hbase tables when hbase is running
if [ -n "$HBASE_FLAG" ]; then
  say "1/6 starting hbase"
  bash scripts/start_hbase.sh
else
  say "1/6 hbase skipped (--no-hbase)"
fi

say "2/6 resetting the last run"
bash scripts/reset_stream.sh

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
#unbuffered, so logs/stream.log is complete even if the job is killed; spark's own
#messages go to logs/stream_spark.log (look there if the job stops with an error)
PYTHONUNBUFFERED=1 spark-submit --packages $KAFKA_PKG streaming/detect_stream.py --kafka-alerts $HBASE_FLAG \
  > "$LOG" 2> "$SPARK_LOG" &
JOB=$!
for i in $(seq 1 150); do
  grep -q "waiting for readings" "$LOG" 2>/dev/null && break
  if ! kill -0 $JOB 2>/dev/null; then
    echo "FAILED: the detection job stopped before it was ready. End of $LOG:"
    tail -15 "$LOG"
    echo "Spark's errors ($SPARK_LOG):"
    grep -E "Error|Exception" "$SPARK_LOG" | tail -10
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

say "5/6 replaying the readings into kafka (at 1 sec per interval: about 10 mins for 6 days)"
python3 streaming/replay.py --interval $INTERVAL $REPLAY_OPTS \
  | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic meter-readings \
      --property parse.key=true --property key.separator='|' > /dev/null
echo "waiting for the detection job to finish (it stops 30 secs after the last reading)..."
if ! wait $JOB; then
  echo "FAILED: the detection job stopped with an error. End of $LOG:"
  tail -15 "$LOG"
  echo "Spark's errors ($SPARK_LOG):"
  grep -E "Error|Exception" "$SPARK_LOG" | tail -10
  exit 1
fi
JOB=""
echo "detection job finished: $(grep -c 'alert(s)$' "$LOG" || true) alert batches, full output in $LOG"
grep "^Readings dropped" "$LOG" || true

say "6/6 evaluating"
spark-submit evaluation/evaluate.py $EVAL_OPTS 2>/dev/null

echo
echo "Run finished."
[ -n "$HBASE_FLAG" ] && echo "HBase queries: bash scripts/hbase_queries.sh"
exit 0
