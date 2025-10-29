#!/bin/bash

# Setup script for KCL .NET integration tests
# This script clones the official amazon-kinesis-client-net and sets up the environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KCL_NET_DIR="$SCRIPT_DIR/amazon-kinesis-client-net"

echo "Setting up KCL .NET integration tests..."

# Clone the official KCL .NET repository if it doesn't exist
if [ ! -d "$KCL_NET_DIR" ]; then
    echo "Cloning amazon-kinesis-client-net..."
    git clone https://github.com/awslabs/amazon-kinesis-client-net.git "$KCL_NET_DIR"
    cd "$KCL_NET_DIR"
    # Checkout the latest release (v2.0)
    git checkout v2.0.0 2>/dev/null || git checkout master
else
    echo "KCL .NET repository already exists at $KCL_NET_DIR"
fi

echo "KCL .NET setup complete!"
echo "Repository location: $KCL_NET_DIR"
echo ""
echo "Next steps:"
echo "1. Build the Java components (requires Java 8+)"
echo "2. Build the .NET components (requires .NET Framework or .NET Core)"
echo "3. Run the integration tests with Docker"
