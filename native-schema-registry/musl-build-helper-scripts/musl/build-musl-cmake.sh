#!/bin/sh

echo "Building native schema registry with musl..."

#apk add --no-cache \
#git curl ca-certificates unzip zip findutils \
#build-base cmake musl-dev clang clang-extra-tools llvm \
#swig lcov protobuf protobuf-dev protoc \

echo $PATH
cd /workspace/native-schema-registry/c
rm -rf build
cmake -S . -B build 
cd build
cmake --build . 