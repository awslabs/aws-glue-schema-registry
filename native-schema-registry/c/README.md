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

## License

**Project License** [Apache License Version 2.0](https://github.com/awslabs/aws-glue-schema-registry/blob/master/LICENSE.txt)

N.B.: Although this repository is released under the Apache-2.0 license, its build dependencies include the third party Swig project. The Swig project's licensing includes the GPL-3.0 license.
