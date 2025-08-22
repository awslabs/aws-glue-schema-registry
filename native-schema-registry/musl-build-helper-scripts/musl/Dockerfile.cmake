FROM  mcr.microsoft.com/dotnet/sdk:8.0-alpine


RUN apk add \
git curl ca-certificates unzip zip findutils \
build-base cmake musl-dev clang clang-extra-tools llvm \
swig lcov protobuf protobuf-dev protoc \


