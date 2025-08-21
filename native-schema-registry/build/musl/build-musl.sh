#!/bin/bash
set -euo pipefail

# Get absolute path three levels up
HOST_SOURCE_DIR="$(realpath ../../..)"
SCRIPT_PATH="$(realpath ./build-musl-inner.sh)"
CONTAINER_WORKDIR="/workspace"
IMAGE="ghcr.io/graalvm/native-image-community:17-muslib"

docker run --rm -it \
  --entrypoint /bin/sh \
  -v "$HOST_SOURCE_DIR":"$CONTAINER_WORKDIR" \
  -v "$SCRIPT_PATH":/tmp/musl-build-inner.sh \
  -w "$CONTAINER_WORKDIR" \
  "$IMAGE" \
  /tmp/musl-build-inner.sh

