# Fail bash if any command fails
set -e

# Start Kafka using docker commnand asynchronously
docker-compose up &
sleep 10

# Run mvn tests
mvn clean install -X

# Clean up docker compose resources if not done automatically.
docker-compose down