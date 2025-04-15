#!/bin/bash

# Make sure Pulsar is running first
echo "Checking if Pulsar is running..."
nc -z localhost 6650 || { 
    echo "Pulsar is not running. Starting Pulsar..."
    cd apache-pulsar-2.10.3
    bin/pulsar standalone &
    sleep 10
    cd ..
}

# Run the producer in one terminal
echo "Starting producer..."
python pulsar_classifier_pipeline.py producer &
PRODUCER_PID=$!

# Wait a moment for producer to connect
sleep 3

# Run the consumer in another terminal
echo "Starting consumer..."
python pulsar_classifier_pipeline.py

# Kill the producer when done
kill $PRODUCER_PID