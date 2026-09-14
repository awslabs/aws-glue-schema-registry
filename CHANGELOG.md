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

## Release 1.1.23
* Upgraded json-schema dependencies version to fix vulnerabilities

## Release 1.1.24
* Upgraded square-wireschema version to fix vulnerabilities

## Release 1.1.25
* Upgraded aws-sdk version to fix vulnerabilities

## Release 1.1.26
* Introduces multilang support for csharp clients

## Release 1.1.27
* Introduce lz4 shim and dependency upgrade to fix vulnerabilities
* Updated local integration tests to make requests to local stack syncrounysly to correct for flakyness

## Release 2.0.0
* **Breaking change.** JSON deserializer no longer resolves the schema's `className` field into a POJO by default; it now returns `JsonDataWithSchema`. Consumers that relied on automatic POJO deserialization will fail on the cast. To restore the previous behavior, set **both** of the following:
  * `jsonClassNameResolutionEnabled=true`
  * `jsonClassNameAllowlist=<comma-separated fully qualified class names>`. Only classes on this list are instantiated. An entry ending in `.*` allows every class directly in that package, so `com.example.pojos.*` avoids listing each POJO. Entries are matched literally rather than as regular expressions, and a bare `*` is rejected. Setting `jsonClassNameResolutionEnabled` alone has no effect, since the allowlist defaults to empty; records whose `className` matches no entry deserialize to `JsonDataWithSchema` and log a WARN.
* **Breaking change.** AWS SDK for Java v1 has been removed from the build. The unused `com.amazonaws:aws-java-sdk-sts` dependency is gone from `serializer-deserializer` and `multilang-schema-registry`, and the `aws.sdk.v1.version` property and `aws-java-sdk-kinesis` dependency management are gone from the parent POM. No published class referenced an SDK v1 type, so library behavior is unchanged, but consumers who were relying on `aws-java-sdk-sts` arriving transitively must now declare it themselves.
* **Breaking change.** Scala 2.12 support has been dropped. `kafka.scala.version` moves from 2.12 to 2.13, so the library is built against the Scala 2.13 Kafka and `mbknor-jackson-jsonschema` artifacts (`kafka_2.13`, `mbknor-jackson-jsonschema_2.13`). Because a single set of artifacts is published and GSR's own coordinates carry no Scala suffix, consumers still on Scala 2.12 must migrate to 2.13; Scala 2.12 and 2.13 are not binary compatible. This aligns with Apache Kafka 4.0, which drops Scala 2.12.
* **Breaking change.** The Kotlin toolchain has moved from 1.9.25 to 2.3.20. `kotlin-stdlib`, `kotlin-stdlib-jdk8`, `kotlin-reflect` and the two Kotlin scripting-compiler artifacts are declared at compile scope, so the version reaches consumers' compile classpaths, where the Kotlin compiler enforces the `Require-Kotlin-Version` manifest attribute. Consumers building with Kotlin 1.9.x should expect to upgrade their Kotlin compiler. Java-only consumers are unaffected, because `javac` ignores that attribute.
* **Security fix.** Upgraded Apache Kafka dependencies from 3.6.1 to 3.9.2 to remediate [CVE-2026-35554](https://nvd.nist.gov/vuln/detail/CVE-2026-35554) (CVSS 8.7 High), a race condition in the Kafka producer buffer pool. This covers all five managed Kafka artifacts: `kafka_2.12`, `kafka-clients`, `kafka-streams`, `connect-api`, and `connect-json`. The serialization wire format is unchanged. `kafka-clients` is a compile-scope dependency, so consumers who pin Kafka 3.6.x themselves will need to resolve the resulting version conflict.
* **Security fix.** Upgraded `square-wireschema` from 5.2.0 to 7.0.1 to remediate [CVE-2026-45799](https://github.com/advisories/GHSA-7xpr-hc2w-34m9) (CVSS 7.5 High), where Wire's `skipGroup()` did not validate negative varint lengths before calling `skip()`, letting a 10-byte payload crash any Wire-decoding service. Every Wire artifact on the dependency tree, including the transitive `wire-runtime` and `wire-runtime-jvm`, now resolves to 7.0.1, which is above the patched 6.3.0 floor.
* **New feature.** The HTTP client used by `AWSSchemaRegistryClient` is now injectable. `GlueSchemaRegistryConfiguration` accepts an HTTP client builder, so consumers can supply their own AWS SDK HTTP client implementation instead of the default. The default remains `UrlConnectionHttpClient`, so existing configurations are unaffected. See the README for usage.
* **Behavior change.** `@SneakyThrows` has been removed to fix a runtime `NoClassDefFoundError: lombok/Lombok`. Checked exceptions are now wrapped in `AWSSchemaRegistryException`, `UncheckedIOException`, or `DataException` depending on the module. Consumers catching specific exception types around serialization or deserialization may need to adjust their catch blocks.
* **Fix.** Removed the compile-scope `kotlinx-serialization-core-jvm` declaration, which resolves the long-standing build failure for consumers on older Kotlin compilers. It was never referenced by GSR source and is needed only at runtime, but compile scope placed its `Require-Kotlin-Version` constraint on every consumer's compile classpath. Also upgraded `okio` and `okio-fakefilesystem` to 3.18.2, which had disagreed with each other at 3.4.0 and 3.2.0.
* **Fix.** The Protobuf Kafka Connect converter no longer throws a `NullPointerException` for STRUCT fields whose schema has no Protobuf metadata, on both the schema conversion and data conversion paths. Note the remaining limitation: a STRUCT built with `SchemaBuilder.struct()` and no `.name(...)` still throws, because the unnamed case is not yet handled.
