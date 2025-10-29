#!/bin/bash

set -e

echo "🚀 Starting AWS Glue Schema Registry C# REAL KCL Integration Tests"

TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$TEST_DIR"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Cleanup function
cleanup() {
    log_step "Cleaning up..."
    docker-compose down -v 2>/dev/null || true
    log_info "✓ Cleanup complete"
}

# Set trap for cleanup
trap cleanup EXIT

# Main execution
main() {
    local test_type="${1:-real-kcl}"
    
    log_step "Starting LocalStack and running tests..."
    
    case "$test_type" in
        "basic")
            log_info "Running basic integration tests..."
            docker-compose up --build --abort-on-container-exit kinesis-basic-test
            ;;
        "real-kcl")
            log_info "Running REAL KCL integration tests with Java MultiLangDaemon..."
            docker-compose up --build --abort-on-container-exit kinesis-real-kcl-test
            ;;
        "all")
            log_info "Running all tests..."
            docker-compose up --build --abort-on-container-exit kinesis-basic-test
            docker-compose up --build --abort-on-container-exit kinesis-real-kcl-test
            ;;
        *)
            log_error "Unknown test type: $test_type"
            echo "Usage: $0 [basic|real-kcl|all]"
            exit 1
            ;;
    esac
    
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        log_info "🎉 Tests completed successfully!"
    else
        log_error "❌ Tests failed with exit code $exit_code"
        exit $exit_code
    fi
}

# Show usage if help requested
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo "AWS Glue Schema Registry C# KCL Integration Tests"
    echo ""
    echo "Usage: $0 [test-type]"
    echo ""
    echo "Test types:"
    echo "  basic     - Run basic integration tests (simulated KCL)"
    echo "  real-kcl  - Run REAL KCL integration tests (Java MultiLangDaemon)"
    echo "  all       - Run all tests"
    echo ""
    echo "Examples:"
    echo "  $0                # Run real KCL tests (default)"
    echo "  $0 basic          # Run basic tests only"
    echo "  $0 real-kcl       # Run real KCL tests only"
    echo "  $0 all            # Run all tests"
    echo ""
    exit 0
fi

main "$@"
