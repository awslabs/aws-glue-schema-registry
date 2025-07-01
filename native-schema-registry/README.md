# Native Schema Registry 

This module provides a native shared library (.so, .dll) version of the AWS Glue Schema Registry SerDes. 
It uses GraalVM to generate the shared library. 



#### Initialize class at build time when building GraalVM Native Image
GraalVM needs to know AOT(ahead-of-time) the reflectively accessed program elements, therefore we
need to supply these elements through build arguments. For example,
```
java.lang.RuntimeException: Unable to get Runnable for removing the FileSystem from the cache when it is closed
```
occurs because `com.google.common.jimfs.JimfsFileSystems` class is accessed through reflection. 

It can be fixed by updating `src/main/resources/META-INF/native-image/software.amazon.glue/native-schema-registry/native-image.properties`
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
