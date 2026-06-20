#!/bin/bash
# SolarMesh Chaos Monkey Script
# Randomly kills a critical service to demonstrate resilience and auto-recovery

SERVICES=("core-api" "trading-engine" "redis" "forecast-service")

echo "🐒 Unleashing Chaos Monkey on SolarMesh..."

while true; do
  # Pick a random service
  TARGET=${SERVICES[$RANDOM % ${#SERVICES[@]}]}
  
  echo "💥 [CHAOS] Attacking container for: $TARGET"
  
  # Find container ID
  CONTAINER_ID=$(docker ps -q -f name=$TARGET)
  
  if [ -n "$CONTAINER_ID" ]; then
    docker kill $CONTAINER_ID > /dev/null
    echo "💀 $TARGET has been terminated."
  else
    echo "⚠️ $TARGET is already down or not running."
  fi
  
  # Wait for a random interval between 30 and 120 seconds
  SLEEP_TIME=$((RANDOM % 90 + 30))
  echo "⏱️ Waiting $SLEEP_TIME seconds before next attack..."
  sleep $SLEEP_TIME
done
