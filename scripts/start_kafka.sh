#!/bin/bash
#starts kafka (one broker, kraft mode, no zookeeper) in the background and
#creates the two topics the project uses. safe to run again.
#run inside the container:  bash scripts/start_kafka.sh
set -e

export KAFKA_HEAP_OPTS="-Xmx512M -Xms512M"   #keep memory low next to hadoop and spark
BOOT=localhost:9092

if jps | grep -q " Kafka$"; then
  echo "Kafka is already running"
else
  echo "Starting Kafka..."
  kafka-server-start.sh -daemon "$KAFKA_HOME/config/kraft/server.properties"
  for i in $(seq 1 30); do
    if kafka-topics.sh --bootstrap-server $BOOT --list >/dev/null 2>&1; then break; fi
    sleep 2
  done
fi

kafka-topics.sh --bootstrap-server $BOOT --create --if-not-exists \
  --topic meter-readings --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server $BOOT --create --if-not-exists \
  --topic alerts --partitions 1 --replication-factor 1

echo
kafka-topics.sh --bootstrap-server $BOOT --describe
