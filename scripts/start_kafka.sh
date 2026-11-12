#!/bin/bash
#starts kafka (one broker, kraft mode, no zookeeper) in the background and
#creates the two topics the project uses. safe to run again.
#run inside the container:  bash scripts/start_kafka.sh
set -eE
trap 'echo; echo "FAILED in start_kafka.sh at line $LINENO: $BASH_COMMAND"; echo "Full details: run it again without 2>/dev/null"' ERR

export KAFKA_HEAP_OPTS="-Xmx512M -Xms512M"   #keep memory low next to hadoop and spark
BOOT=localhost:9092

if jps | grep -q " Kafka$"; then
  echo "Kafka is already running"
else
  echo "Starting Kafka..."
  kafka-server-start.sh -daemon "$KAFKA_HOME/config/kraft/server.properties"
  for i in $(seq 1 30); do
    if kafka-topics.sh --bootstrap-server $BOOT --list > /dev/null 2>&1; then break; fi
    sleep 2
  done
fi

if ! kafka-topics.sh --bootstrap-server $BOOT --list > /dev/null 2>&1; then
  echo
  echo "FAILED: Kafka did not answer on $BOOT within 60 seconds."
  echo "Check: jps should list Kafka. Log: $KAFKA_HOME/logs/server.log"
  exit 1
fi

kafka-topics.sh --bootstrap-server $BOOT --create --if-not-exists \
  --topic meter-readings --partitions 3 --replication-factor 1 > /dev/null
kafka-topics.sh --bootstrap-server $BOOT --create --if-not-exists \
  --topic alerts --partitions 1 --replication-factor 1 > /dev/null

#show only the project's two topics (not kafka's internal ones)
echo
echo "Kafka is ready. Topics:"
for t in meter-readings alerts; do
  n=$(kafka-topics.sh --bootstrap-server $BOOT --describe --topic $t | grep -c "Partition: " || true)
  if [ "$n" -lt 1 ]; then echo "FAILED: topic $t was not created"; exit 1; fi
  echo "  $t  ($n partition$( [ "$n" -gt 1 ] && echo s ))"
done
