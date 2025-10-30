#!/bin/bash
set -euo pipefail

# Default to linux/arm64 for musl builds, but allow override
PLATFORM=${PLATFORM:-linux/arm64}

echo "Building native schema registry with musl for platform: $PLATFORM"

# Ensure buildx is available and use it for proper multi-platform support
if docker buildx version &> /dev/null; then
    echo "Using docker buildx for multi-platform support"
    DOCKER_BUILD_CMD="docker buildx build --load"
else
    echo "Warning: docker buildx not available, falling back to regular docker build"
    DOCKER_BUILD_CMD="docker build"
fi

$DOCKER_BUILD_CMD --platform "$PLATFORM" -f Dockerfile.graalvm -t native-graalvm .
$DOCKER_BUILD_CMD --platform "$PLATFORM" -f Dockerfile.cmake -t native-cmake .
# Get absolute path three levels up
HOST_SOURCE_DIR="$(realpath ../../..)"
SCRIPT_PATH="$(realpath ./build-musl-inner.sh)"
CMAKE_SCRIPT_PATH="$(realpath ./build-musl-cmake.sh)"
CONTAINER_WORKDIR="/workspace"
IMAGE="ghcr.io/graalvm/native-image-community:21-muslib"

docker run --rm -it \
  --platform "$PLATFORM" \
  --entrypoint /bin/sh \
  -v "$HOST_SOURCE_DIR":"$CONTAINER_WORKDIR" \
  -v "$SCRIPT_PATH":/tmp/musl-build-inner.sh \
  -w "$CONTAINER_WORKDIR" \
  "native-graalvm" \
  /tmp/musl-build-inner.sh

echo "Building cmake"
docker run --rm -it \
  --platform "$PLATFORM" \
  -v "$HOST_SOURCE_DIR":"$CONTAINER_WORKDIR" \
  -v "$CMAKE_SCRIPT_PATH":/tmp/musl-build-cmake.sh \
  -w "$CONTAINER_WORKDIR" \
  "native-cmake" \
  /tmp/musl-build-cmake.sh
