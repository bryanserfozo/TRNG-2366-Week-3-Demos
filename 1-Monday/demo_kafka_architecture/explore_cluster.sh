#!/bin/bash
# Kafka Architecture Exploration - CLI Demo Script
# This script demonstrates how to inspect a Kafka cluster using CLI tools

echo "=============================================="
echo "KAFKA CLUSTER EXPLORATION"
echo "=============================================="
echo ""

# Define Kafka container and bootstrap server
KAFKA_CONTAINER="kafka-broker"
BOOTSTRAP_SERVER="localhost:9092"

echo "1. BROKER INFORMATION"
echo "----------------------------------------------"
echo "Listing broker configuration..."
docker exec $KAFKA_CONTAINER kafka-broker-api-versions \
    --bootstrap-server $BOOTSTRAP_SERVER 2>/dev/null | head -20
echo ""

echo "2. TOPIC LIST (Initial State)"
echo "----------------------------------------------"
echo "Listing all topics..."
docker exec $KAFKA_CONTAINER kafka-topics \
    --list \
    --bootstrap-server $BOOTSTRAP_SERVER
echo ""

echo "3. CREATING A TEST TOPIC"
echo "----------------------------------------------"
echo "Creating topic 'demo-architecture' with 3 partitions..."
docker exec $KAFKA_CONTAINER kafka-topics \
    --create \
    --topic demo-architecture \
    --partitions 3 \
    --replication-factor 1 \
    --bootstrap-server $BOOTSTRAP_SERVER 2>/dev/null
echo "Topic created!"
echo ""

echo "4. DESCRIBING THE TOPIC"
echo "----------------------------------------------"
echo "Getting detailed topic information..."
docker exec $KAFKA_CONTAINER kafka-topics \
    --describe \
    --topic demo-architecture \
    --bootstrap-server $BOOTSTRAP_SERVER
echo ""

echo "5. CLUSTER METADATA"
echo "----------------------------------------------"
echo "Displaying cluster metadata..."
docker exec $KAFKA_CONTAINER kafka-metadata \
    --snapshot /var/lib/kafka/data/__cluster_metadata-0/00000000000000000000.log \
    --command "print" 2>/dev/null | head -30 || echo "Metadata command not available in this Kafka version"
echo ""

echo "=============================================="
echo "EXPLORATION COMPLETE"
echo "=============================================="
