#!/bin/bash
# setup_topics.sh
# Run this on EC2 after Kafka starts to create all required topics

KAFKA_HOME="/home/ubuntu/kafka_2.13-3.7.0"
BOOTSTRAP="localhost:9092"

echo "Creating Kafka topics for FIH..."

$KAFKA_HOME/bin/kafka-topics.sh --create --if-not-exists \
  --topic match.events \
  --bootstrap-server $BOOTSTRAP \
  --partitions 2 \
  --replication-factor 1

echo "Verifying topics..."
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP

echo "Done!"