#!/bin/sh

# Script to extract native shared object version from POM file for C# builds
# Usage: extract-native-so-version.sh [path_to_pom]

POM_PATH="${1:-../../../../pom.xml}"

if [ ! -f "$POM_PATH" ]; then
    echo "Error: POM file not found at $POM_PATH" >&2
    exit 1
fi

# Extract version using grep/sed approach to get native shared object version
VERSION=$(grep -E '<version>.*</version>' "$POM_PATH" | head -1 | sed 's/.*<version>\([^<]*\)<\/version>.*/\1/' | tr -d ' \t')

if [ -z "$VERSION" ]; then
    echo "Error: Could not extract version from $POM_PATH" >&2
    exit 1
fi

echo "$VERSION"
