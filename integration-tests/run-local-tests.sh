#!/usr/bin/env bash
# Fail bash if any command fails
set -e

# Kafka Connect worker image
CONNECT_IMAGE=apache/kafka:3.8.0
# Suffix for this run's Glue schema names, so schema versions left by earlier runs cannot fail it
RUN_ID=$(date +%y%m%d%H%M%S)-$RANDOM
# Per-run copies of the connector configs and the plugin directories
CONNECT_WORK_DIR=target/kafka-connect

### Begin with a clean start
cleanUpDockerResources() {
    docker kill $(docker ps -q)
    docker rm $(docker ps -a -q)
    docker network prune -f
    docker system prune --volumes -f
}

setUpMongoDBLocal() {
    mongo_image=public.ecr.aws/d4c7g6k3/mongo

    docker pull ${mongo_image}
    ## Set up dockerized local cluster of MongoDB with 3 replicas
    ## MongoDB Source connector leverage staging replica and it's necessary to have replicas for source connector to
    # work
    docker network create local-mongodb
    docker run -d --net local-mongodb -p 27017:27017 --name mongo1 ${mongo_image} mongod --replSet repl-set --port 27017
    docker run -d --net local-mongodb -p 27018:27018 --name mongo2 ${mongo_image} mongod --replSet repl-set --port 27018
    docker run -d --net local-mongodb -p 27019:27019 --name mongo3 ${mongo_image} mongod --replSet repl-set --port 27019

    echo "Waiting for startup.."
    sleep 5

    ## Populate data in mongo
    docker exec -it mongo1 mongo --eval "
    rs.initiate(
      {
        _id : 'repl-set',
        members: [
          { _id : 0, host : \"mongo1:27017\", \"priority\": 2 },
          { _id : 1, host : \"mongo2:27018\", \"priority\": 0 },
          { _id : 2, host : \"mongo3:27019\", \"priority\": 0 }
        ]
      }, { force: true }
    )
    "
    echo "Waiting for replica set to propagate.."
    echo "Populating data in mongoDB.."
    sleep 30
    docker exec -it mongo1 mongo --eval '
        db.createCollection("fruits")
        db.fruits.insertMany([ {name: "banana", origin: "mexico", price: 1} ])
        db.fruits.insertMany([ {name: "apple", origin: "canada", price: 2} ])
        db.fruits.insertMany([ {name: "guava", origin: "india", price: 3} ])
     '
}

downloadMongoDBConnector() {
    MONGO_CONNECTOR=mongo-kafka-connect-1.3.0-all.jar
    if [ -f "$MONGO_CONNECTOR" ]; then
        echo "$MONGO_CONNECTOR exists, not downloading.."
    else
        echo "$MONGO_CONNECTOR does not exist, downloading.."
        wget -O mongo-kafka-connect-1.3.0-all.jar https://search.maven.org/remotecontent?filepath=org/mongodb/kafka/mongo-kafka-connect/1.3.0/mongo-kafka-connect-1.3.0-all.jar
    fi
    ## mongo-kafka-connect 1.3.0 needs Avro 1.9.2 at runtime but does not bundle it
    MONGO_CONNECTOR_AVRO=avro-1.9.2.jar
    if [ -f "$MONGO_CONNECTOR_AVRO" ]; then
        echo "$MONGO_CONNECTOR_AVRO exists, not downloading.."
    else
        echo "$MONGO_CONNECTOR_AVRO does not exist, downloading.."
        wget -O ${MONGO_CONNECTOR_AVRO} https://repo1.maven.org/maven2/org/apache/avro/avro/1.9.2/avro-1.9.2.jar
    fi
}

AVRO_CONVERTER=schema-registry-kafkaconnect-converter-*.jar
JSON_SCHEMA_CONVERTER=jsonschema-kafkaconnect-converter-*.jar
PROTOBUF_SCHEMA_CONVERTER=protobuf-kafkaconnect-converter-*.jar

## Each converter and the Mongo connector get their own plugin directory, so the worker loads each
## with an isolated classloader instead of putting them on its classpath
copyGSRConverters() {
    rm -rf ${CONNECT_WORK_DIR}/plugins
    mkdir -p ${CONNECT_WORK_DIR}/plugins/avro ${CONNECT_WORK_DIR}/plugins/json \
        ${CONNECT_WORK_DIR}/plugins/protobuf ${CONNECT_WORK_DIR}/plugins/mongo
    cp ../avro-kafkaconnect-converter/target/${AVRO_CONVERTER} ${CONNECT_WORK_DIR}/plugins/avro/
    cp ../jsonschema-kafkaconnect-converter/target/${JSON_SCHEMA_CONVERTER} ${CONNECT_WORK_DIR}/plugins/json/
    cp ../protobuf-kafkaconnect-converter/target/${PROTOBUF_SCHEMA_CONVERTER} ${CONNECT_WORK_DIR}/plugins/protobuf/
    cp ${MONGO_CONNECTOR} ${MONGO_CONNECTOR_AVRO} ${CONNECT_WORK_DIR}/plugins/mongo/
}

## Copies the connector configs for this run and suffixes the Glue schema names with RUN_ID
prepareConnectConfigs() {
    rm -rf ${CONNECT_WORK_DIR}/${1}
    cp -r kafka-connect-${1}-configs ${CONNECT_WORK_DIR}/${1}
    sed -E -i.bak "s/^((key|value)\.converter\.schemaName=.*)$/\1-${RUN_ID}/" \
        ${CONNECT_WORK_DIR}/${1}/worker.properties ${CONNECT_WORK_DIR}/${1}/file-sink-standalone.properties
    rm -f ${CONNECT_WORK_DIR}/${1}/*.bak
}

startKafkaConnectTasks() {
    ### Run the source and sink connector tasks in a Kafka Connect worker.
    ### FileStreamSink is not on the worker classpath since Kafka 3.2, so it is copied to a plugin directory.
    docker pull ${CONNECT_IMAGE}
    docker run -d --user root --net=host \
    -v "$(pwd)/${CONNECT_WORK_DIR}/${1}":/connect \
    -v "$(pwd)/${CONNECT_WORK_DIR}/plugins/${1}":/plugins/gsr:ro \
    -v "$(pwd)/${CONNECT_WORK_DIR}/plugins/mongo":/plugins/mongo:ro \
    -v $HOME/.aws/credentials:/root/.aws/credentials:ro \
    --entrypoint bash ${CONNECT_IMAGE} \
    -c "mkdir -p /plugins/connect-file && cp /opt/kafka/libs/connect-file-*.jar /plugins/connect-file/ && \
    cd /connect && \
    /opt/kafka/bin/connect-standalone.sh worker.properties mongo-source-standalone.properties \
    file-sink-standalone.properties"
}

validateSinkOutput() {
    ### Assert if sink file is generated and records exist
    SINK_FILE=${CONNECT_WORK_DIR}/${1}/sink-file.txt
    if [ -f "$SINK_FILE" ]; then
        echo "$SINK_FILE was generated successfully, validating contents.."
        count=$(wc -l < "$SINK_FILE")
        if [[ $count -eq 3 ]]; then
            echo '-------------\n### KAFKA CONNECT TEST SUCCESSFUL\n-------------';
        else
            echo '-------------\n### KAFKA CONNECT TEST FAILED\n-------------'
        fi
    else
        echo '-------------\n### KAFKA CONNECT TEST FAILED\n-------------'
    fi
}

## Deletes the Glue schemas this run registered for the format
deleteConnectSchemas() {
    region=$(sed -n 's/^value\.converter\.region=//p' ${CONNECT_WORK_DIR}/${1}/worker.properties)
    for schemaName in $(sed -E -n 's/^(key|value)\.converter\.schemaName=//p' ${CONNECT_WORK_DIR}/${1}/worker.properties); do
        echo "Deleting Glue schema ${schemaName}.."
        aws glue delete-schema --region ${region} \
            --schema-id SchemaName=${schemaName},RegistryName=default-registry > /dev/null \
            || echo "Could not delete Glue schema ${schemaName}"
    done
}

cleanUpConnectFiles() {
    rm -rf ${CONNECT_WORK_DIR}/${1}
}

cleanUpDockerResources || true
# Start Kafka using docker command asynchronously
docker-compose up --no-attach localstack &
sleep 10
## Run mvn tests for Kafka and Kinesis Platforms
## This includes testProduceConsumeWithKafkaLZ4Compression which verifies
## that at.yawk.lz4:lz4-java works correctly with Kafka's native LZ4 compression
cd .. && mvn --file integration-tests/pom.xml verify -Psurefire -X && cd integration-tests
cleanUpDockerResources

## Use ECR Public with anonymous login
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws

### -------------------------------
### Kafka Connect Integration tests
### -------------------------------
downloadMongoDBConnector
copyGSRConverters

runConnectTests() {
    docker-compose up --no-attach localstack &
    setUpMongoDBLocal
    prepareConnectConfigs ${1}
    startKafkaConnectTasks ${1}
    echo "Waiting for Sink task to pick up data.."
    sleep 60
    validateSinkOutput ${1}
    ## Clean-up
    deleteConnectSchemas ${1}
    cleanUpConnectFiles ${1}
    cleanUpDockerResources
}

for format in avro json protobuf
do
    runConnectTests ${format}
done

## Clean up docker compose resources if not done automatically.
cleanUpDockerResources
