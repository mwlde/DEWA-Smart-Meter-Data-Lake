#!/bin/bash
#benchmark: the same night-flow leak check run as a mapreduce job, a hive query and a
#spark batch job (in local mode and on yarn) on the raw zone, each timed several times, then compared with each
#other, with the streaming job's alerts and with the injected leaks.
#run inside the container, from /home/hadoop/dewa (hadoop running, raw zone loaded):
#  bash scripts/run_benchmark.sh        (3 runs of each tool)
#  bash scripts/run_benchmark.sh 5      (5 runs of each tool)
#each tool's own output goes to logs/benchmark_<tool>.log
set -eE -o pipefail
trap 'echo; echo "FAILED in run_benchmark.sh at line $LINENO: $BASH_COMMAND"' ERR
cd "$(dirname "$0")/.."

RUNS=${1:-3}
BASE=/user/hadoop/dewa
OUT=$BASE/benchmark
TIMES=logs/benchmark_times.csv
JAR=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1)
mkdir -p logs

say() { echo; echo "=== $1"; }

say "checking hdfs, the raw zone and the tools"
if ! hdfs dfs -test -d $BASE/raw/readings 2>/dev/null; then
  echo "FAILED: no raw zone at $BASE/raw/readings (or hdfs is not running)"
  echo "What to do: bash scripts/load_raw_zone.sh 2>/dev/null, then hive -f hive/01_raw_zone.sql 2>/dev/null"
  exit 1
fi
if [ -z "$JAR" ]; then
  echo "FAILED: hadoop streaming jar not found under \$HADOOP_HOME/share/hadoop/tools/lib"
  exit 1
fi
#make sure hive sees every dt=... folder (untimed)
if ! hive -e "USE dewa; MSCK REPAIR TABLE raw_readings;" > logs/benchmark_hive_setup.log 2>&1; then
  echo "FAILED: hive could not open the raw_readings table. End of logs/benchmark_hive_setup.log:"
  tail -15 logs/benchmark_hive_setup.log
  echo "What to do: hive -f hive/01_raw_zone.sql 2>/dev/null"
  exit 1
fi
echo "ok ($RUNS runs of each tool)"

echo "tool,run,seconds" > $TIMES

timed() {   #timed NAME command...  runs the command RUNS times, records each wall-clock time
  local name=$1; shift
  say "$name"
  for r in $(seq 1 $RUNS); do
    hdfs dfs -rm -r -f -skipTrash $OUT/$name > /dev/null 2>&1 || true
    local start end secs
    start=$(date +%s.%N)
    if ! "$@" > logs/benchmark_$name.log 2>&1; then
      echo "FAILED: $name, run $r. End of logs/benchmark_$name.log:"
      tail -20 logs/benchmark_$name.log
      exit 1
    fi
    end=$(date +%s.%N)
    secs=$(awk "BEGIN { printf \"%.2f\", $end - $start }")
    echo "$name,$r,$secs" >> $TIMES
    echo "  run $r: $secs s"
  done
}

timed mapreduce hadoop jar "$JAR" \
  -files benchmark/leak_mapper.py,benchmark/leak_reducer.py \
  -mapper "python3 leak_mapper.py" -reducer "python3 leak_reducer.py" \
  -input "$BASE/raw/readings/dt=*/readings.csv" -output $OUT/mapreduce
timed hive hive -f benchmark/leak_check.hql
timed spark_batch spark-submit benchmark/spark_batch.py
#on yarn as well, so spark pays the same job start-up cost as mapreduce and hive.
#optional: if yarn cannot run it, the benchmark carries on without this row
if ! (timed spark_batch_yarn spark-submit --master yarn benchmark/spark_batch.py --out-name spark_batch_yarn); then
  echo "  (spark on yarn skipped; details in logs/benchmark_spark_batch_yarn.log)"
  grep -v "^spark_batch_yarn," $TIMES > $TIMES.tmp && mv $TIMES.tmp $TIMES
fi

say "comparing the results"
spark-submit benchmark/compare.py --times $TIMES 2>/dev/null
