# Fail bash if any command fails
set -e

# Start Kafka using docker command asynchronously
docker-compose up &
sleep 10

# Run mvn tests by downloading latest snapshot dependency versions
cd .. && mvn "-Dtest=GlueSchemaRegistryKinesisIntegrationTest#testProduceConsumeSingleRecordWithKPLAndKCL,
GlueSchemaRegistryKafkaIntegrationTest#testProduceConsumeMultipleDataFormatRecords"  --file integration-tests/pom.xml verify -Psurefire -U