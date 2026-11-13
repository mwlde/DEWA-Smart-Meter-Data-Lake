#!/bin/bash
#restart test: does the streaming job lose or repeat anything when it crashes mid-stream?
#runs the same replay twice, on 1,000 meters for 3 days, in a separate lake folder
#(/user/hadoop/dewa_restart_test), so the main lake and its results are not touched:
#  run A  without interruption
#  run B  the detection job killed with kill -9 mid-stream (no clean stop), then
#         restarted from its checkpoint while the replay carries on into kafka
#then evaluates both and compares their detection, alert windows and landing zones
#(evaluation/compare_runs.py). about 15 mins. hbase is not used.
#run inside the container, from /home/hadoop/dewa:
#  bash scripts/restart_test.sh
#options:
#  --kill-after N   secs into run B's replay to kill the job (default 110: 03:30 on the
#                   second live day, in the middle of the night's leak windows)
set -eE -o pipefail
trap 'echo; echo "FAILED in restart_test.sh at line $LINENO: $BASH_COMMAND"; pkill -9 -f "streaming/detect_stream.py" 2>/dev/null; echo "Logs: logs/restart_a.log, logs/restart_b.log (alerts), logs/restart_*_spark.log (spark errors)"' ERR
cd "$(dirname "$0")/.."

KILL_AFTER=110
while [ $# -gt 0 ]; do
  case "$1" in
    --kill-after) KILL_AFTER=$2; shift ;;
    *) echo "FAILED: unknown option $1"; exit 1 ;;
  esac
  shift
done
TEST=/user/hadoop/dewa_restart_test
DATA=data/restart_test
BOOT=localhost:9092
KAFKA_PKG=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6
mkdir -p logs

say() { echo; echo "=== $1"; }

#a detection job left from another run would read the same kafka topic
if pgrep -f "streaming/detect_stream.py" > /dev/null; then
  echo "FAILED: a detection job is already running"
  echo "What to do: stop it with: pkill -f streaming/detect_stream.py, then run this again"
  exit 1
fi

#empty kafka topics, so each run reads only its own replay
reset_kafka() {
  for t in meter-readings alerts; do
    kafka-topics.sh --bootstrap-server $BOOT --delete --topic $t > /dev/null 2>&1 || true
  done
  sleep 5
  bash scripts/start_kafka.sh | grep -v "^$"
}

#starts the detection job on lake $1, logging to $2; waits until it has printed
#"waiting for readings" $3 times (2 after a restart, as the log is appended)
start_job() {
  PYTHONUNBUFFERED=1 spark-submit --packages $KAFKA_PKG streaming/detect_stream.py --base "$1" --kafka-alerts \
    >> "$2" 2>> "${2%.log}_spark.log" &
  JOB=$!
  for i in $(seq 1 150); do
    [ "$(grep -c 'waiting for readings' "$2" || true)" -ge "$3" ] && return 0
    if ! kill -0 $JOB 2>/dev/null; then
      echo "FAILED: the detection job stopped before it was ready. End of $2:"
      tail -15 "$2"
      exit 1
    fi
    sleep 2
  done
  echo "FAILED: the detection job was not ready after 5 minutes. End of $2:"
  tail -15 "$2"
  exit 1
}

#waits for the detection job to stop by itself (30 secs after the last reading)
finish_job() {
  if ! wait $JOB; then
    echo "FAILED: the detection job stopped with an error. End of $1:"
    tail -15 "$1"
    exit 1
  fi
  echo "detection job finished: $(grep -c 'alert(s)$' "$1" || true) alert batches"
  grep "^Readings dropped" "$1" || true
}

#same replay as scripts/run_stream.sh: one interval a second, 1% of readings late
replay() {
  python3 streaming/replay.py --data $DATA --interval 1 --late-share 0.01 --late-max 2 \
    | kafka-console-producer.sh --bootstrap-server $BOOT --topic meter-readings \
        --property parse.key=true --property key.separator='|' > /dev/null
}

say "1/7 making the test data (1,000 meters, 3 days, in $DATA)"
python3 data_generator/generate.py --meters 1000 --days 3 --out $DATA | grep -v ": done$"

say "2/7 loading it into $TEST (the main lake is not touched)"
hdfs dfs -rm -r -f -skipTrash $TEST > /dev/null
hdfs dfs -mkdir -p $TEST/a/raw/meters $TEST/a/raw/readings $TEST/a/raw/ground_truth $TEST/a/raw/vacancies $TEST/b
hdfs dfs -put -f $DATA/meters.csv $TEST/a/raw/meters/
hdfs dfs -put -f $DATA/ground_truth.csv $TEST/a/raw/ground_truth/
hdfs dfs -put -f $DATA/vacancies.csv $TEST/a/raw/vacancies/
for day in $DATA/readings/dt=*; do
  hdfs dfs -put -f "$day" $TEST/a/raw/readings/
done
spark-submit streaming/baseline.py --base $TEST/a 2>/dev/null > logs/restart_baseline.log \
  || { cat logs/restart_baseline.log; exit 1; }
grep "Baseline saved" logs/restart_baseline.log
#run B gets an identical copy: same raw zone, same baseline
hdfs dfs -cp $TEST/a/raw $TEST/a/curated $TEST/b/
echo "lakes ready: $TEST/a and $TEST/b"

say "3/7 run A: without interruption"
reset_kafka
: > logs/restart_a.log; : > logs/restart_a_spark.log
start_job $TEST/a logs/restart_a.log 1
echo "detection job ready at $(date +%T); replaying 2 live days (about 3.5 mins)"
replay 2> logs/restart_a_replay.log
grep -E "^(Late readings|Replay finished)" logs/restart_a_replay.log || true
finish_job logs/restart_a.log

say "4/7 run B: killing the detection job after $KILL_AFTER secs, then restarting it"
reset_kafka
: > logs/restart_b.log; : > logs/restart_b_spark.log
start_job $TEST/b logs/restart_b.log 1
echo "detection job ready at $(date +%T); replaying the same 2 days"
replay 2> logs/restart_b_replay.log &
REPLAY=$!
sleep $KILL_AFTER
echo "$(date +%T) killing the detection job with kill -9 (no clean stop); the replay had $(grep '^sent' logs/restart_b_replay.log | tail -1)"
pkill -9 -f "streaming/detect_stream.py" || true
wait $JOB 2>/dev/null || true
echo "  micro-batches the job had started before it was killed: $(grep -c 'alert(s)$' logs/restart_b.log || true) with alerts"
sleep 3
echo "$(date +%T) restarting it from its checkpoint (the replay carries on into kafka meanwhile)"
echo "=== restarted ===" >> logs/restart_b.log
start_job $TEST/b logs/restart_b.log 2
echo "$(date +%T) restarted job ready; it now catches up on the readings that arrived while it was down"
wait $REPLAY
grep -E "^(Late readings|Replay finished)" logs/restart_b_replay.log || true
finish_job logs/restart_b.log

say "5/7 evaluating run A"
spark-submit evaluation/evaluate.py --base $TEST/a 2>/dev/null | tee logs/restart_a_eval.txt

say "6/7 evaluating run B"
spark-submit evaluation/evaluate.py --base $TEST/b 2>/dev/null | tee logs/restart_b_eval.txt

say "7/7 comparing the two runs"
spark-submit evaluation/compare_runs.py --a $TEST/a --b $TEST/b 2>/dev/null | tee logs/restart_test_results.txt

echo
echo "Restart test finished. Output saved to logs/restart_test_results.txt"
echo "The test lake stays in $TEST; remove it with: hdfs dfs -rm -r -skipTrash $TEST"
