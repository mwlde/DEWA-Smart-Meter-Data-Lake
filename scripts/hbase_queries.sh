#!/bin/bash
#runs the example hbase queries (hbase/queries.hbase) and hides hbase's own
#start-up log lines, which it prints to the normal output (so 2>/dev/null
#alone does not remove them). errors from the queries are still shown.
#run inside the container, after a streaming run with --hbase has finished:
#  bash scripts/hbase_queries.sh
cd "$(dirname "$0")/.."

if ! curl -sf http://localhost:8080/version/cluster > /dev/null; then
  echo "FAILED: HBase is not running. Start it with: bash scripts/start_hbase.sh"
  exit 1
fi

hbase shell -n hbase/queries.hbase 2>/dev/null \
  | grep -v -E '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:,]+ +(INFO|WARN|DEBUG) |^SLF4J'
status=${PIPESTATUS[0]}
if [ "$status" -ne 0 ]; then
  echo
  echo "FAILED: the HBase shell stopped with an error (exit code $status)."
  echo "Full details: hbase shell -n hbase/queries.hbase"
  exit 1
fi
