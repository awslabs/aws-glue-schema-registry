#!/bin/bash
set -e
cd "$(dirname "$0")"
echo "Starting tests at $(date)" | tee test-run.log
docker-compose up --build --abort-on-container-exit kinesis-real-kcl-test 2>&1 | tee -a test-run.log
docker-compose down -v
echo "Tests completed at $(date)" | tee -a test-run.log
