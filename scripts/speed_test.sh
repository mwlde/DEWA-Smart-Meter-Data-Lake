#!/bin/bash
#replay-speed test: runs the stream twice on the same data, at 1 and at 4 real secs
#per 15-min interval (no hbase), then compares the latencies.
#a check that does not wait for later readings has the same latency at both speeds;
#one that waits gets slower by the intervals it waits (see evaluation/speed_compare.py).
#it makes its own small dataset (1,000 meters, 3 days: 2 live days) and loads it into
#the raw zone, replacing what was in data/ and the raw zone. about 25 mins.
#run inside the container, from /home/hadoop/dewa:
#  bash scripts/speed_test.sh
set -eE -o pipefail
trap 'echo; echo "FAILED in speed_test.sh at line $LINENO: $BASH_COMMAND"' ERR
cd "$(dirname "$0")/.."

echo "=== making the test data (1,000 meters, 3 days)"
python3 data_generator/generate.py --meters 1000 --days 3
bash scripts/load_raw_zone.sh 2>/dev/null
hdfs dfs -rm -r -f -skipTrash /user/hadoop/dewa/speed_test > /dev/null 2>&1 || true

echo
echo "=== fast run: 1 sec per interval"
bash scripts/run_stream.sh --no-hbase --interval 1 --tag fast
echo
echo "=== slow run: 4 secs per interval"
bash scripts/run_stream.sh --no-hbase --interval 4 --tag slow
echo
echo "=== comparing"
spark-submit evaluation/speed_compare.py --fast /user/hadoop/dewa/speed_test/fast \
  --slow /user/hadoop/dewa/speed_test/slow --fast-interval 1 --slow-interval 4 2>/dev/null
