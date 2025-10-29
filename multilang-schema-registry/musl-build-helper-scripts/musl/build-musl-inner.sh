#!/bin/bash
set -euo pipefail

## Install dependencies
#microdnf install -y maven git unzip zip curl bash findutils ca-certificates tar gzip
#microdnf install -y dnf dnf-plugins-core
#dnf -y config-manager --set-enabled ol9_codeready_builder
#dnf -y install oracle-epel-release-el9
#dnf -y --enablerepo=ol9_developer_EPEL install swig lcov gcc clang clang-tools-extra make cmake
#dnf clean all && rm -rf /var/cache/dnf
#
## Build zlib
#cd /tmp
#curl -L https://github.com/madler/zlib/releases/download/v1.3/zlib-1.3.tar.gz | tar -xz
#cd zlib-1.3
#CC=x86_64-linux-musl-gcc CFLAGS="-fPIC -O2" ./configure --static
#make
#cp libz.a /usr/local/musl/lib/libz.a

# Build Java lib
cd /workspace
# First generate protobuf classes
mvn clean compile -Dcheckstyle.skip=true || true
# Then build without tests to avoid protobuf compilation issues
mvn -U clean install -Dcheckstyle.skip=true -DskipTests || true

# Build C lib
cd /workspace/native-schema-registry/c
cmake -S . -B build -DCMAKE_C_COMPILER=x86_64-linux-musl-gcc -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_TARGET=musl-cross -DDISABLE_QUALITY_GATES=ON
cd build
cmake --build . || true

# Build native image
cd /workspace/native-schema-registry
mvn install -P native-image-musl -DskipTests -Dcheckstyle.skip=true 
