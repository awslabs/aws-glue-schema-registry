# Native Schema Registry in C 

This module provides a C language based API for the schema registry serializer / de-serializers.

## Build
We use CMake to build the targets in this module.

### Compile
```asm
#Run in c directory

cmake -S. -Bbuild 
cd build 
cmake --build .

#### Clean
cmake --build . --target clean
```
### Testing
```asm
ctest .
#Re-run failed tests with verbose output
ctest --rerun-failed --output-on-failure
```

### Code Analysis
Code is statically analyzed using clang-tidy.

### Coverage
Code coverage checks using gcov and lcov and fail if the coverage is below threshold.

#### Installation
You might have to install these modules using your OS package manager.

### Sanitizers
We use address,leak sanitizers to detect memory leaks and any potential issues during build. As of now, they only work on Linux.

### Platform Support

TBD

## Develop

### Add Class
In order to add a class that is accessible by target language, these steps are required:
1. Create the .h file in `include` directory
2. Create the .i file in `src/swig` directory
3. Create the .c file in `src` directory 
4. Add the .i file to `glue_schema_registry_serde.i` file in `src/swig` directory
5. Add the .c file to the `CMakeLists.txt` file in `src` directory

## License

**Project License** [Apache License Version 2.0](https://github.com/awslabs/aws-glue-schema-registry/blob/master/LICENSE.txt)

N.B.: Although this repository is released under the Apache-2.0 license, its build dependencies include the third party Swig project. The Swig project's licensing includes the GPL-3.0 license.
