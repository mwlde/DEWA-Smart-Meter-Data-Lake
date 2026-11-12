#!/bin/bash
#starts hbase (standalone mode) and its rest server, then creates the project's
#tables if they do not exist yet. safe to run again.
#run inside the container:  bash scripts/start_hbase.sh
#
#tables:
#  meter_readings  row key = meter id + "#" + reversed timestamp, so a prefix
#                  scan on one meter returns its newest readings first.
#                  column family r (ts, value, unit, cumulative, meter_type, area).
#  meter_alerts    row key = meter id + "#" + alert type: one row per open problem.
#                  column family a keeps 10 versions, so repeat alerts form a history.
set -eE
trap 'echo; echo "FAILED in start_hbase.sh at line $LINENO: $BASH_COMMAND"; echo "Full details: run it again without 2>/dev/null"' ERR

REST=http://localhost:8080
export HBASE_HEAPSIZE=1G
LOG=$HBASE_HOME/logs/start_hbase.out   #hbase's start-up chatter goes here, not the screen
mkdir -p "$HBASE_HOME/logs"
: > "$LOG"

quiet() {   #quiet "description" command...  runs the command with its output in $LOG
  local what=$1; shift
  if ! "$@" >> "$LOG" 2>&1; then
    echo
    echo "FAILED: $what"
    echo "Last lines of $LOG:"
    tail -15 "$LOG"
    exit 1
  fi
}

if jps | grep -q " HMaster$"; then
  echo "HBase is already running"
else
  echo "Starting HBase..."
  quiet "start-hbase.sh did not start HBase" start-hbase.sh
fi

if jps | grep -q " RESTServer$"; then
  echo "HBase REST server is already running"
else
  echo "Starting the HBase REST server on port 8080..."
  quiet "the REST server did not start" hbase-daemon.sh start rest -p 8080
fi

echo "Waiting for HBase to answer..."
for i in $(seq 1 45); do
  if curl -sf $REST/version/cluster > /dev/null; then break; fi
  sleep 2
done
if ! curl -sf $REST/version/cluster > /dev/null; then
  echo
  echo "FAILED: the HBase REST server did not answer on port 8080 within 90 seconds."
  echo "Check: jps should list HMaster and RESTServer. Logs: $HBASE_HOME/logs"
  echo "Last lines of $LOG:"
  tail -15 "$LOG"
  exit 1
fi

create_if_missing() {   #create_if_missing TABLE "create statement"
  if curl -sf $REST/$1/schema > /dev/null; then
    echo "Table $1 already exists"
  else
    echo "Creating table $1"
    echo "$2" | hbase shell -n >> "$LOG" 2>&1 || true
    if ! curl -sf $REST/$1/schema > /dev/null; then
      echo "FAILED: table $1 was not created. Last lines of $LOG:"
      tail -15 "$LOG"
      exit 1
    fi
  fi
}

create_if_missing meter_readings "create 'meter_readings', {NAME => 'r', VERSIONS => 1}"
create_if_missing meter_alerts   "create 'meter_alerts', {NAME => 'a', VERSIONS => 10}"

echo
echo "HBase is ready: tables meter_readings and meter_alerts"
