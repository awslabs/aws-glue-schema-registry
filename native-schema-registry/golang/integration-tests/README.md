# Integration Tests

This directory contains integration tests for the Go native schema registry implementation. Tests can be run in two ways:

## Option 1: Host-based Testing (Default)

Run tests directly on the host system with Kafka services in Docker:

```bash
# Run integration tests
make test-integ

# Run with coverage
make cover-test-integ
```

## Option 2: Alpine Container Testing (New)

Run tests inside an Alpine Linux container for complete isolation:

```bash
# Run integration tests in Alpine container
make test-integ-alpine

# Run with coverage in Alpine container
make cover-test-integ-alpine

# Build Alpine test image only
make build-alpine-test-image

# Clean up Alpine containers and images
make clean-alpine
```

## Alpine Container Benefits

- **Isolation**: Tests run in a completely clean environment every time
- **Consistency**: Same environment across different developer machines and CI/CD
- **Reproducibility**: Easy to reproduce test issues locally
- **Go 1.20**: Uses Alpine's Go 1.20 package for exact version compatibility

## Environment Variables

Both testing approaches support these environment variables:

- `AWS_REGION`: AWS region for GSR (default: us-east-1)
- `AWS_ACCESS_KEY_ID`: AWS access key
- `AWS_SECRET_ACCESS_KEY`: AWS secret key
- `AWS_SESSION_TOKEN`: AWS session token (if using temporary credentials)
- `KAFKA_BROKER`: Kafka broker address (default: localhost:9092 for host tests, kafka:9092 for container tests)

## Test Output

- Host tests: Results in `build/integration-test-output.txt`
- Alpine tests: Results in `build/alpine-integration-test-output.txt`
- Coverage reports: Generated in `build/coverage/` directory

## Architecture

### Host-based Testing Flow
1. Start Kafka/Zookeeper with Docker Compose
2. Generate protobuf files on host
3. Run Go tests on host connecting to containerized Kafka
4. Clean up containers

### Alpine Container Testing Flow
1. Start Kafka/Zookeeper with Docker Compose
2. Build Alpine container with Go 1.20 and dependencies
3. Generate protobuf files inside container
4. Run Go tests inside container connecting to Kafka
5. Mount build directory for results
6. Clean up all containers

Both approaches produce compatible test results and support the same test suites.
