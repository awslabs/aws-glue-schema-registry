# Fail bash if any command fails
set -e

# Start Kafka using docker commnand asynchronously
docker-compose up &
sleep 10

# Run mvn tests by downloading latest snapshot dependency versions
mvn clean install -U -X