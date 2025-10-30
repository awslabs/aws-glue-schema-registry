# Support multi-platform builds - ARG must be before FROM
ARG TARGETPLATFORM
FROM --platform=$TARGETPLATFORM mcr.microsoft.com/dotnet/sdk:8.0-alpine

# Set UTF-8 locale to handle Unicode filenames (including emojis)
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Re-import TARGETPLATFORM after FROM
ARG TARGETPLATFORM

RUN apk add \
git curl ca-certificates unzip zip findutils \
build-base cmake musl-dev clang clang-extra-tools llvm \
swig lcov protobuf protobuf-dev protoc \
