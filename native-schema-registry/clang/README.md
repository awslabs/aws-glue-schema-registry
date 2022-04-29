# Native Schema Registry in Clang 

This module provides a C language based API for the schema registry serializer / de-serializers.

##Build
We use CMake to build the targets in this module.

###Compile
```asm
#Run in clang directory

cmake --build build --target clean 
cmake -S. -Bbuild 
cd build 
cmake --build .
```
###Testing
```asm
ctest --rerun-failed --output-on-failure
```

###Sanitizers
We use address,leak sanitizers to detect memory leaks and any potential issues during build. As of now, they don't work on OSX

###Platform Support

TBD

