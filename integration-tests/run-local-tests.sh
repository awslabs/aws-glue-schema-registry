#!/usr/bin/env bash
# Fail bash if any command fails
set -e

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
}

AVRO_CONVERTER=schema-registry-kafkaconnect-converter-*.jar
JSON_SCHEMA_CONVERTER=jsonschema-kafkaconnect-converter-*.jar

copyGSRConverters() {
    cp ../avro-kafkaconnect-converter/target/${AVRO_CONVERTER} .
    cp ../jsonschema-kafkaconnect-converter/target/${JSON_SCHEMA_CONVERTER} .
}

startKafkaConnectTasks() {
    if [ $1 == 'avro' ]; then
        CONVERTER_JAR=${AVRO_CONVERTER}
    else if [ $1 == 'json' ]; then
        CONVERTER_JAR=${JSON_SCHEMA_CONVERTER}
        fi
    fi

    ### Get inside the kafka_connect container and run source and sink connector tasks
    docker pull public.ecr.aws/d4c7g6k3/1ambda/kafka-connect:latest
    docker run -e CONNECT_BOOTSTRAP_SERVERS="kafka:9092" -d -it -v "$(pwd)":/integration-tests \
    -v $HOME/.aws/credentials:/root/.aws/credentials:ro \
    --net=host public.ecr.aws/d4c7g6k3/1ambda/kafka-connect:latest bash \
    -c "unset JMX_PORT;\\
    cd /integration-tests/kafka-connect-${1}-configs;\\
    cp ../${CONVERTER_JAR} /opt/kafka_2.11-0.10.0.0/libs/;\\
    cp ../mongo-kafka-connect-1.3.0-all.jar /opt/kafka_2.11-0.10.0.0/libs/;\\
    stop /opt/kafka_2.11-0.10.0.0/bin/connect-standalone.sh;\\
    nohup /opt/kafka_2.11-0.10.0.0/bin/connect-standalone.sh worker.properties mongo-source-standalone.properties \\
    file-sink-standalone.properties"
}

validateSinkOutput() {
    ### Assert if sink file is generated and records exist
    SINK_FILE=kafka-connect-${1}-configs/sink-file.txt
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

cleanUpConnectFiles() {
    rm -rf kafka-connect-${1}-configs/sink-file.txt
    rm -rf kafka-connect-${1}-configs/standalone.offsets
}

cleanUpDockerResources || true
# Start Kafka using docker command asynchronously
docker-compose up &
sleep 10
## Run mvn tests for Kafka and Kinesis Platforms
mvn verify -Psurefire -X
cleanUpDockerResources

## Use ECR Public with anonymous login
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws

### -------------------------------
### Kafka Connect Integration tests
### -------------------------------
downloadMongoDBConnector
copyGSRConverters

runConnectTests() {
    docker-compose up &
    setUpMongoDBLocal
    startKafkaConnectTasks ${1}
    echo "Waiting for Sink task to pick up data.."
    sleep 60
    validateSinkOutput ${1}
    ## Clean-up
    cleanUpConnectFiles ${1}
    cleanUpDockerResources
}

for format in avro json
do
    runConnectTests ${format}
done

## Clean up docker compose resources if not done automatically.
cleanUpDockerResources
