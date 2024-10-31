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

## Release 1.1.2
* Introduce cache to improve serialization performance
* Add DatumReader Cache to improve de-serialization performance
* Reduce logging
* Add additional examples of configuring Kafka Connect and clarification on what property names are expected
* Fix resource clean up in Kafka integration test

## Release 1.1.3
* Modify UserAgent to emit usage metrics
* Add tests to include key and value schemas both 

## Release 1.1.4
* Upgrade Apache Kafka version to 2.8.1

## Release 1.1.5
* Fix security vulnerability in transitive dependencies
* Remove configuration logging information

## Release 1.1.9
* Added Support for Protobuf Format
* Improved the caching mechanism to improve availability of the serializer and deserializer

## Release 1.1.10
* Fix bug for missing Protobuf wellknown types
* Fix Json schema converter NPEs due to missing connect.index and connect.type for sink only cases
* Add AWS SDK dependency to allow irsa service account

## Release 1.1.11
* Add support for Kafka Connect Protobuf converter

## Release 1.1.12
* Upgraded Avro Version to prevent a CVE

## Release 1.1.13
* Upgraded kotlin dependency versions to prevent a CVE

## Release 1.1.14
* Upgraded Protobuf dependency version to prevent a CVE
* Upgraded everit-json-schema dependency version to prevent a CVE

## Release 1.1.15
* Upgrade Avro, Apicurio and Localhost utils versions

## Release 1.1.16
* Upgraded Wire version
* Excluded some transitive dependencies that are having vulnerabilities

## Release 1.1.17
* Upgraded kafka dependencies version

## Release 1.1.18
* Add a dummy class in the serializer-deserializer-msk-iam module for javadoc and source jar generation
* Upgraded Avro and Json dependencies version
* Upgraded AWS SDK v1 and v2 versions to fix vulnerabilities

## Release 1.1.19
* Upgraded dependency versions to remove ION dependencies

## Release 1.1.20
* Upgrade the dependency version to remove commons:compress dependency

## Release 1.1.21
* Upgraded Avro dependencies version to fix vulnerabilities

## Release 1.1.22
* Upgraded protobuf dependencies version to fix vulnerabilities
