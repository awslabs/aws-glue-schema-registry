#!/bin/bash

set -e

echo "Starting Kinesis GSR Integration Tests with LocalStack + KCL .NET..."

# Start LocalStack and run tests
echo "Starting LocalStack and running tests..."
docker-compose up --build --abort-on-container-exit

# Cleanup
echo "Cleaning up..."
docker-compose down

echo "Kinesis GSR KCL Integration Tests completed!"
