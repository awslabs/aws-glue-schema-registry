# AWS Glue Schema Registry Sample Code

This repository contains code samples for integrating with Glue Schema Registry.

## Kinesis Data Streams PutRecords / GetRecords

Glue Schema Registry can be used with PutRecords / GetRecords APIs of Kinesis Data Streams to encode / decode data. See `PutRecordGetRecordExample` code on how to integrate.

### Running sample code

Create a stream on Kinesis Data Streams.

```
aws kinesis create-stream --stream-name testStream --shard-count 1 --region us-west-2
```

Build the code.
```
mvn clean install
```

Run the sample code.
```
mvn exec:java -Dexec.mainClass="com.amazonaws.services.schemaregistry.examples.kds.PutRecordGetRecordExample" -Dexec.args="--region us-west-2 --stream testStream --numRecords 5 --schema testGsrSchema"
```

