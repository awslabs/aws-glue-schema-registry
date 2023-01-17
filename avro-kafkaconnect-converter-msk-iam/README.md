
# AWS Glue Schema Registry Kafka Connect converters with Amazon MSK IAM Authentication

**AWS Glue Schema Registry** provides a solution for customers to centrally discover, control and evolve 
schemas while ensuring data produced was validated by registered schemas.

**AWS Glue Schema Registry Library** offers Serializers and Deserializers that plug-in with Glue Schema Registry.

**The Amazon MSK Library for AWS Identity and Access Management** enables developers to use AWS Identity and Access Management (IAM) to connect to their Amazon Managed Streaming for Apache Kafka (Amazon MSK) clusters.

## Features

1. Messages/records are serialized / deserialized on Kafka connectors by using 
schema-registry-serde.
2. Support for AVRO format.
3. Support for IAM authentication to Amazon MSK clusters.
4. Records can be compressed to reduce message size.
5. An inbuilt local in-memory cache to save calls to AWS Glue Schema Registry. The schema version id for a schema 
definition is cached on Producer side and schema for a schema version id is cached on the Consumer side.
6. Auto registration of schema can be enabled for any new schema to be auto-registered.
7. For Schemas, Evolution check is performed while registering.

## Building from Source

After you've downloaded the code from GitHub, you can build it using Maven.

The following maven command will clean the target directory, compile the project, execute the tests and package the project build into a JAR.

`cd avro-kafkaconnect-converter-msk-iam && mvn clean package`

## Installing

Copy the `schema-registry-kafkaconnect-converter-msk-iam-{release-number}.jar` to the `lib` directory of your Kafka Connector.

## Example:

### Source Kafka Connector (debezium-mysql) with AVRO format and IAM Authentication

```json
{

    "name": "debezium-mysql",
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.user": "user",
    "database.server.id": "123456",
    "tasks.max": "1",
    "database.history.kafka.topic": "<db history topic>",
    "database.history.kafka.bootstrap.servers": "<bootstrap servers>",
    "database.server.name": "<server name>",
    "database.port": "3306",
    "include.schema.changes": "true",
    "database.hostname": "<database host name>",
    "database.password": "<password>",
    "database.include.list": "<include regex>",
    "value.converter.schemas.enable": "false",
    "key.converter.schemas.enable": "false",
    "database.history.consumer.security.protocol": "SASL_SSL",
    "database.history.consumer.sasl.mechanism": "AWS_MSK_IAM",
    "database.history.consumer.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "database.history.consumer.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "database.history.producer.security.protocol":"SASL_SSL",
    "database.history.producer.sasl.mechanism": "AWS_MSK_IAM",
    "database.history.producer.sasl.jaas.config" : "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "database.history.producer.sasl.client.callback.handler.class" : "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
    "value.converter.region": "us-east-1",
    "value.converter.schemaAutoRegistrationEnabled": "true",
    "value.converter.avroRecordType": "GENERIC_RECORD"
}
```

 ## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.
