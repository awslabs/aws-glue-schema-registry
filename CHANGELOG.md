# Changelog
## Release 1.0.0
* Initial Release

## Release 1.0.1
* Added more documentation
* Reduced logging
* Added flexibility to schema naming
* Added Kinesis Data Streams usage examples
* Added integration tests

## Release 1.1.0
* Added Support for JSONSchema Format.
* Added Validation logic while using encode method for calls through KPL.
* Generalized Kafka Specific Serializer/Deserializer to a data format agnostic classes like 
GlueSchemaRegistryKafkaSerializer/GlueSchemaRegistryKafkaDeserializer.
* Generalized AWSKafkaAvroSerDe to GlueSchemaRegistryKafkaSerDe for it to be used for multiple data formats.
* Using better convention for poms and maven inheritance.
* Added JSON Kafka Converter.
* Improved integration tests to run with local dockerized streaming systems.

## Release 1.1.1
* Fixed checkstyle errors with maven build in integration-tests folder.
* Reduced number of Canaries tests.
* Removed jitpack as a repo for everit and using maven central to pull everit.