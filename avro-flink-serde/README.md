## This module is not recommended
Please check out [Apache Flink AWS Connectors](https://github.com/apache/flink-connector-aws) repository for the latest support: [Avro SerializationSchema and DeserializationSchema](https://github.com/apache/flink-connector-aws/tree/main/flink-formats-aws/flink-avro-glue-schema-registry) and [JSON SerializationSchema and Deserialization](https://github.com/apache/flink-connector-aws/tree/main/flink-formats-aws/flink-json-glue-schema-registry). Protobuf integration will be followed up soon.

## Instructions

The recommended way to use the AWS Glue Schema Registry Flink Connector for Java is to consume it from Maven.

**Minimum requirements** &mdash; Apache Flink versions supported **Flink 1.11+**

**Working with Kinesis Data Analytics** &mdash; AWS Glue Schema Registry can be setup with [Amazon Kinesis Data 
Analytics 
for Apache Flink](https://docs.aws.amazon.com/kinesisanalytics/latest/java/what-is.html).

For using Amazon VPC with Kinesis Data Analytics please see [Configuring Kinesis Data Analytics for Apache Flink 
inside Amazon VPC.](https://docs.aws.amazon.com/kinesisanalytics/latest/java/vpc.html) 

### Maven Dependency
  ``` xml
  <dependency>
       <groupId>software.amazon.glue</groupId>
       <artifactId>schema-registry-flink-serde</artifactId>
       <version>1.1.24/version>
  </dependency>
  ```
### Code Example

#### Flink Kafka Producer with AVRO format

```java
    String topic = "topic";
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test");

    Map<String, Object> configs = new HashMap<>();
    configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
    configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(new File("path/to/avro/file"));

    FlinkKafkaProducer<GenericRecord> producer = new FlinkKafkaProducer<>(
            topic,
            GlueSchemaRegistryAvroSerializationSchema.forGeneric(schema, topic, configs),
            properties);
    stream.addSink(producer);
```

#### Flink Kafka Consumer with AVRO format

```java
    String topic = "topic";
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test");

    Map<String, Object> configs = new HashMap<>();
    configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-east-1");
    configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());

    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(new File("path/to/avro/file"));

    FlinkKafkaConsumer<GenericRecord> consumer = new FlinkKafkaConsumer<>(
            topic,
            GlueSchemaRegistryAvroDeserializationSchema.forGeneric(schema, configs),
            properties);
    DataStream<GenericRecord> stream = env.addSource(consumer);
```
