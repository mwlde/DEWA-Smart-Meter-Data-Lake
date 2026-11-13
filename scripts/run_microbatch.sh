#!/bin/bash
#micro-batch experiment: could mapreduce do the streaming job's work by running
#every 15 minutes? each cycle runs, as hadoop streaming jobs on yarn:
#  leak   the night-flow leak check on the last hour of readings (4 interval files)
#  stuck  the electricity stuck check on the last 6 hours (24 interval files)
#and times them, start-up included. mapreduce keeps nothing between runs, so every
#cycle reads its whole window again: the stuck job reads 24 intervals to add one.
#the cycles cover 02:00 to 03:45 (8 cycles) on the second live day, so both windows are full.
#run inside the container, from /home/hadoop/dewa, with hadoop running and data/ generated
#(at least 3 days):
#  bash scripts/run_microbatch.sh        (8 cycles)
#  bash scripts/run_microbatch.sh 4      (4 cycles)
#results: logs/microbatch_times.csv and logs/microbatch_results.md
set -eE -o pipefail
trap 'echo; echo "FAILED in run_microbatch.sh at line $LINENO: $BASH_COMMAND"' ERR
cd "$(dirname "$0")/.."

CYCLES=${1:-8}
BASE=/user/hadoop/dewa/microbatch
SLOTS=/tmp/dewa_slots
TIMES=logs/microbatch_times.csv
JAR=$(ls $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1)
mkdir -p logs

say() { echo; echo "=== $1"; }

say "checking hdfs, the data and the tools"
if [ ! -f data/meters.csv ]; then
  echo "FAILED: no generated data in $(pwd)/data"
  echo "What to do: python3 data_generator/generate.py"
  exit 1
fi
if ! hdfs dfsadmin -report > /dev/null 2>&1; then
  echo "FAILED: HDFS is not running (jps should list NameNode and DataNode)"
  echo "What to do: on your computer, docker restart hadoop"
  exit 1
fi
if [ -z "$JAR" ]; then
  echo "FAILED: hadoop streaming jar not found under \$HADOOP_HOME/share/hadoop/tools/lib"
  exit 1
fi
echo "ok"

say "writing one file per 15-minute interval for the first two live days (untimed)"
#in production each file would land in hdfs as its interval arrives
rm -rf $SLOTS
python3 streaming/replay.py --interval 0 --days 2 --to-dir $SLOTS 2> /dev/null
FILES=($(ls $SLOTS/*.csv | sort))
if [ ${#FILES[@]} -lt 192 ]; then
  echo "FAILED: expected 192 interval files, got ${#FILES[@]} (the data needs at least 3 days)"
  exit 1
fi
hdfs dfs -rm -r -f -skipTrash $BASE > /dev/null 2>&1 || true
hdfs dfs -mkdir -p $BASE/slots
hdfs dfs -put -f $SLOTS/*.csv $BASE/slots/
echo "${#FILES[@]} interval files in $BASE/slots, about $(du -k ${FILES[0]} | cut -f1) KB each"

#first cycle: 02:00 on the second live day = interval 96 + 8
FIRST=104
echo "check,cycle,data_up_to,input_files,input_kb,seconds,alerts" > $TIMES

run_cycle() {   #run_cycle CHECK CYCLE WINDOW_FILES MAPPER REDUCER
  local check=$1 cycle=$2 window=$3 mapper=$4 reducer=$5
  local end=$((FIRST + cycle)) inputs=() kb=0 i
  for i in $(seq $((end - window)) $((end - 1))); do
    inputs+=(-input "$BASE/slots/$(basename ${FILES[$i]})")
    kb=$((kb + $(du -k ${FILES[$i]} | cut -f1)))
  done
  local out=$BASE/out/${check}_$cycle start secs
  start=$(date +%s.%N)
  if ! hadoop jar "$JAR" -files benchmark/$mapper,benchmark/$reducer \
      -mapper "python3 $mapper" -reducer "python3 $reducer" \
      "${inputs[@]}" -output $out > logs/microbatch_$check.log 2>&1; then
    echo "FAILED: $check, cycle $cycle. End of logs/microbatch_$check.log:"
    tail -20 logs/microbatch_$check.log
    exit 1
  fi
  secs=$(awk "BEGIN { printf \"%.2f\", $(date +%s.%N) - $start }")
  local alerts upto
  alerts=$(hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l)
  upto=$(basename ${FILES[$((end - 1))]} .csv)
  echo "$check,$cycle,$upto,$window,$kb,$secs,$alerts" >> $TIMES
  printf "  %-5s cycle %d (data up to %s): %s s, %d files read, %d alert(s)\n" \
    "$check" "$cycle" "$upto" "$secs" "$window" "$alerts"
}

say "running $CYCLES cycles (each = one 15-minute interval arriving)"
for c in $(seq 0 $((CYCLES - 1))); do
  run_cycle leak $c 4 leak_mapper.py leak_reducer.py
  run_cycle stuck $c 24 stuck_mapper.py stuck_reducer.py
done

say "summary"
python3 - "$TIMES" <<'EOF'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1])))
lines = ["| Check | Runtime per cycle (median) | Range | Intervals read per cycle | New intervals per cycle | Data read per cycle |",
         "| --- | --- | --- | --- | --- | --- |"]
for check in ("leak", "stuck"):
    r = [x for x in rows if x["check"] == check]
    secs = [float(x["seconds"]) for x in r]
    kb = statistics.median(int(x["input_kb"]) for x in r)
    lines.append(f"| {check} | {statistics.median(secs):.1f} s | {min(secs):.1f} to {max(secs):.1f} s | "
                 f"{r[0]['input_files']} | 1 | {kb / 1024:.1f} MB |")
total = statistics.median(float(x["seconds"]) for x in rows if x["check"] == "leak") + \
        statistics.median(float(x["seconds"]) for x in rows if x["check"] == "stuck")
lines += ["",
          f"Both checks together: about {total:.0f} s of YARN jobs per 15-minute cycle, run back to back.",
          "Every cycle is a new job: 96 cycles a day per check, 480 jobs a day for the five checks.",
          "Each reading is read 4 times by the leak check and 24 times by the stuck check "
          "(96 times for a 24-hour rule); the streaming job reads it once and keeps the window in memory."]
text = "\n".join(lines)
print(text)
open("logs/microbatch_results.md", "w").write("# Micro-batch experiment: MapReduce every 15 minutes\n\n" + text + "\n")
EOF
echo
echo "Saved: $TIMES and logs/microbatch_results.md"
