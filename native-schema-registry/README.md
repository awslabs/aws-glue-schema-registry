# Native Schema Registry 

This module provides a native shared library (.so, .dll) version of the AWS Glue Schema Registry SerDes. 
It uses GraalVM to generate the shared library. 

## Build 

Requires GraalVM (21.0+) with native-image support.

The C data types module needs to be built before building the Java module.

```asm
cd c && cmake -S. -Bbuild
cd build && cmake --build . --target native_schema_registry_c_data_types
cd ../../ && mvn package -P native-image
```