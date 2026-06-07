# Native Schema Registry 

This module provides a native shared library (.so, .dll) version of the AWS Glue Schema Registry SerDes. 
It uses GraalVM to generate the shared library. 

## Build command for multi-lang GSR java layer
mvn install -P native-image

Usually, the process is as follows:
1. go to c/ directory
2. run cmake -S. -Bbuild
3. run cmake --build build

This will produce the libnativeschemaregistry.so and other necessary shared libraries.

On Amazon Linux, you might need to install cmake, Python, lcov, llvm etc and set PATHs accordingly to make it work. These steps are not necessary on Ubnutu as they come in-built.

The versions we've used when building the native schema registry library are:
- Maven 3.8.7
- Java (GraalVM Community Edition) 21.0.2
- gcov 13.3.0
- llvm 18.1.3
- lcov 2.0-1
- cmake 3.28.3
- dotnet 8.0.117

-------------

## Language specific README's
Once the native shared object is generated, please look at the individual language READMEs to build the corresponding multilang client.
1. C#: [[C#](csharp/AWSGsrSerDe/README.md)]

#### Initialize class at build time when building GraalVM Native Image
GraalVM needs to know AOT(ahead-of-time) the reflectively accessed program elements, therefore we
need to supply these elements through build arguments. For example,
```
java.lang.RuntimeException: Unable to get Runnable for removing the FileSystem from the cache when it is closed
```
occurs because `com.google.common.jimfs.JimfsFileSystems` class is accessed through reflection. 

It can be fixed by updating `src/main/resources/META-INF/native-image/software.amazon.glue/multilang-schema-registry/native-image.properties`
file with following argument
```properties
Args = --initialize-at-build-time=com.google.common.jimfs.JimfsFileSystems
```
Refer to this [guide](https://www.graalvm.org/22.0/reference-manual/native-image/Reflection/) 
for more detail

#### Accessing Resources In native image
By default, GraalVM Native Image does not integrate any resources into native-executables. Therefore,
we will need to specify the resources we would like to integrate in `src/main/resources/resource-config.json`.

For example,
```json
{
  "resources": {
    "includes": [
      {
        "pattern": ".*/.*proto$"
      }
    ]
  }
}

```
will allow us to include all the resource files that ends with proto when we build our Native Image

Refer to this [guide](https://www.graalvm.org/22.1/reference-manual/native-image/Resources/) for more detail.
