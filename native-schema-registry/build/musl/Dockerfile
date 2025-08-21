# ==========================
# Stage 1 — Java build (GraalVM musl)
# ==========================
FROM ghcr.io/graalvm/native-image-community:17-muslib AS java-build

WORKDIR /tmp

# Build PIC-compatible zlib to fix shared library linking
RUN curl -L https://github.com/madler/zlib/releases/download/v1.3/zlib-1.3.tar.gz | tar -xz && \
    cd zlib-1.3 && \
    CC=x86_64-linux-musl-gcc CFLAGS="-fPIC -O2" ./configure --static && \
    make && \
    cp libz.a /usr/local/musl/lib/libz.a

# Copy the project
COPY . .

# Install packages for Java build
RUN microdnf install -y maven git unzip zip curl bash findutils ca-certificates tar gzip \
 && microdnf clean all

# Install packages for C build
RUN microdnf install -y dnf dnf-plugins-core && \
    dnf -y config-manager --set-enabled ol9_codeready_builder && \
    dnf -y install oracle-epel-release-el9 && \
    dnf -y --enablerepo=ol9_developer_EPEL install swig lcov gcc clang clang-tools-extra make cmake && \
    dnf clean all && rm -rf /var/cache/dnf

# Build core Java library
RUN mvn -U clean install -Dcheckstyle.skip=true -DskipTests
RUN pwd

# TODO: remove this workaround when we have a straightforward way to build java first and then c
# Build  C native libraries - just for java build to work as there is a dependency on c headers
WORKDIR /tmp/native-schema-registry/c
RUN cmake -S . -B build \
  -DCMAKE_C_COMPILER=x86_64-linux-musl-gcc \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON 
WORKDIR /tmp/native-schema-registry/c/build
RUN cmake --build . || true

# Build Java native-image profile
WORKDIR /tmp/native-schema-registry
RUN mvn clean install -P native-image-musl -DskipTests -Dcheckstyle.skip=true

# ==========================
# Stage 2 — Alpine latest (C + C# builds)
# ==========================
FROM alpine:latest

WORKDIR /tmp
COPY --from=java-build /tmp /tmp

# Install dependencies for C and C#
RUN apk add --no-cache \
    bash git curl ca-certificates unzip zip findutils \
    build-base cmake musl-dev clang clang-extra-tools llvm \
    swig lcov protobuf protobuf-dev protoc \
    dotnet8-sdk

# --- C build ---
WORKDIR /tmp/native-schema-registry/c
RUN rm -rf build
RUN cmake -S . -B build && cmake --build build

# --- C# build ---

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_SESSION_TOKEN
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
ENV AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN

WORKDIR /tmp/native-schema-registry/csharp/AWSGsrSerDe

# fails because protoc doesn't work with Alpine (musl libc based image)
RUN dotnet clean . && dotnet build . || true

# workaround because protoc from grpc.tools is not compatible with Alpine
# this is only for the protoc compilation, and doesnt not affect functionality of the library
RUN apk update \
    && apk --no-cache add libc6-compat \
    && apk --no-cache add \ 
    -X https://dl-cdn.alpinelinux.org/alpine/v3.17/main \
    -X https://dl-cdn.alpinelinux.org/alpine/v3.17/community \
    protobuf=3.21.9-r0 \
    && cd /root/.nuget/packages/grpc.tools/2.48.1/tools/linux_x64 \
    && rm protoc \
    && ln -s /usr/bin/protoc protoc \
    && chmod +x grpc_csharp_plugin

# RUN ls -l /root/.nuget/packages/grpc.tools/2.48.1/tools/linux_x64/ && \
#     file /root/.nuget/packages/grpc.tools/2.48.1/tools/linux_x64/protoc || true && \
#     ldd /root/.nuget/packages/grpc.tools/2.48.1/tools/linux_x64/protoc || true

# need to build it twice due to protobuf compilation misorder (normal for csharp builds today)
RUN dotnet build . || true 
RUN dotnet build . && dotnet test . || true

