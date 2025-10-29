#!/bin/bash

set -e

echo "🚀 Starting AWS Glue Schema Registry C# Kinesis Integration Tests"

# Configuration
LOCALSTACK_ENDPOINT="http://localhost:4566"
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KCL_NET_DIR="$TEST_DIR/amazon-kinesis-client-net"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        log_error "Docker is required but not installed"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        log_error "Docker Compose is required but not installed"
        exit 1
    fi
    
    log_info "✓ Prerequisites check passed"
}

# Setup KCL .NET
setup_kcl_net() {
    log_info "Setting up Amazon Kinesis Client Library for .NET..."
    
    if [ ! -d "$KCL_NET_DIR" ]; then
        log_info "Cloning amazon-kinesis-client-net..."
        git clone https://github.com/awslabs/amazon-kinesis-client-net.git "$KCL_NET_DIR"
    else
        log_info "KCL .NET already exists, pulling latest..."
        cd "$KCL_NET_DIR"
        git pull origin main || git pull origin master
        cd "$TEST_DIR"
    fi
    
    log_info "✓ KCL .NET setup complete"
}

# Start LocalStack
start_localstack() {
    log_info "Starting LocalStack..."
    
    cd "$TEST_DIR"
    docker-compose up -d localstack
    
    # Wait for LocalStack to be ready
    log_info "Waiting for LocalStack to be ready..."
    timeout=60
    while [ $timeout -gt 0 ]; do
        if curl -s "$LOCALSTACK_ENDPOINT/health" | grep -q "kinesis.*available"; then
            log_info "✓ LocalStack is ready"
            return 0
        fi
        sleep 2
        timeout=$((timeout - 2))
    done
    
    log_error "LocalStack failed to start within timeout"
    exit 1
}

# Build test application
build_test_app() {
    log_info "Building C# test application..."
    
    cd "$TEST_DIR"
    docker run --rm \
        -v "$TEST_DIR:/app" \
        -w /app \
        mcr.microsoft.com/dotnet/sdk:8.0 \
        dotnet build KinesisGsrTest/KinesisGsrTest.csproj
    
    log_info "✓ Test application built successfully"
}

# Run integration tests
run_integration_tests() {
    log_info "Running integration tests..."
    
    cd "$TEST_DIR"
    
    # Run tests in Docker container with network access to LocalStack
    docker run --rm \
        --network kinesis_default \
        -v "$TEST_DIR:/app" \
        -w /app \
        -e LOCALSTACK_ENDPOINT="http://localstack:4566" \
        -e AWS_ACCESS_KEY_ID="test" \
        -e AWS_SECRET_ACCESS_KEY="test" \
        -e AWS_DEFAULT_REGION="us-east-1" \
        mcr.microsoft.com/dotnet/sdk:8.0 \
        dotnet test KinesisGsrTest/KinesisGsrTest.csproj --logger "console;verbosity=detailed"
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        log_info "✅ All integration tests passed!"
    else
        log_error "❌ Integration tests failed with exit code $exit_code"
        return $exit_code
    fi
}

# Cleanup
cleanup() {
    log_info "Cleaning up..."
    
    cd "$TEST_DIR"
    docker-compose down -v
    
    log_info "✓ Cleanup complete"
}

# Main execution
main() {
    trap cleanup EXIT
    
    check_prerequisites
    setup_kcl_net
    start_localstack
    build_test_app
    run_integration_tests
    
    log_info "🎉 Integration test suite completed successfully!"
}

# Handle script arguments
case "${1:-}" in
    "setup")
        check_prerequisites
        setup_kcl_net
        ;;
    "start")
        start_localstack
        ;;
    "build")
        build_test_app
        ;;
    "test")
        run_integration_tests
        ;;
    "cleanup")
        cleanup
        ;;
    *)
        main
        ;;
esac
