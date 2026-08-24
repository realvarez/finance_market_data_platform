#!/bin/bash
set -e

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

create_topic() {
    local topic=$1
    local partitions=$2
    local retention_ms=$3

    kafka-topics --create \
        --if-not-exists \
        --bootstrap-server "$BOOTSTRAP" \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config retention.ms="$retention_ms"
}

echo "Creating Kafka topics on $BOOTSTRAP..."

create_topic "market.ticks"               3 604800000    # 7 days
create_topic "market.candles.raw"         3 2592000000   # 30 days
create_topic "market.candles.calculated"  3 2592000000   # 30 days
create_topic "market.candles.reconciled"  3 7776000000   # 90 days
create_topic "market.signals"             1 2592000000   # 30 days
create_topic "market.errors"              1 604800000    # 7 days

# Confluent Server internals (telemetry exporter) auto-create their topic with
# replication factor 3, which is impossible on this single-broker cluster and
# spams INVALID_REPLICATION_FACTOR every few seconds. The cp-server configure
# script does not reliably translate the disable flags from env, so pre-create
# the topic at RF=1 instead: once it exists the exporter stays silent.
create_topic "_confluent-telemetry-metrics" 1 86400000   # 1 day

echo "Topics created:"
kafka-topics --list --bootstrap-server "$BOOTSTRAP"
