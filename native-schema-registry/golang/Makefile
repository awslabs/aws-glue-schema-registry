# Variables
BUILD_DIR := build
COVERAGE_DIR := $(BUILD_DIR)/coverage
PKG_DIR := $(BUILD_DIR)/pkg

# Default target
.PHONY: all
all: build

# Create build directories
.PHONY: setup-dirs
setup-dirs:
	@mkdir -p $(BUILD_DIR)
	@mkdir -p $(COVERAGE_DIR)
	@mkdir -p $(PKG_DIR)

# Build target - compile/validate module and create package artifacts
.PHONY: build
build: setup-dirs
	@echo "Building Go module..."
	go build -o $(PKG_DIR)/ ./...
	go mod download
	go mod tidy
	@echo "Build complete. Artifacts in $(BUILD_DIR)/"

# Coverage wrapper using pattern rule
.PHONY: cover
cover:
	@echo "Usage: make cover-<target>"
	@echo "Available coverage targets:"
	@echo "  make cover-test        # Unit + component tests with coverage"
	@echo "  make cover-test-integ  # Integration tests with coverage" 
	@echo "  make cover-test-all    # All tests with coverage"

# Pattern rule for coverage - matches "cover-<anything>"
cover-%: setup-dirs
	@echo "Running $* with coverage..."
	$(MAKE) $* COVERAGE=true

# Unit + Component tests (excludes integration-tests/ directory only)
.PHONY: test
test: setup-dirs
ifdef COVERAGE
	@echo "Running unit and component tests with coverage..."
	go test -v -coverprofile=$(COVERAGE_DIR)/unit-coverage.out $(shell go list ./... | grep -v integration-tests)
	go tool cover -html=$(COVERAGE_DIR)/unit-coverage.out -o $(COVERAGE_DIR)/unit-coverage.html
	@echo "Unit and component test coverage report: $(COVERAGE_DIR)/unit-coverage.html"
else
	@echo "Running unit and component tests..."
	go test -v $(shell go list ./... | grep -v integration-tests) 2>&1 | tee $(BUILD_DIR)/unit-test-output.txt
endif

# Integration tests with Docker (integration-tests/ directory only)
.PHONY: test-integ
test-integ: setup-dirs
ifdef COVERAGE
	@echo "Running integration tests with coverage..."
	cd integration-tests && docker compose up -d
	cd integration-tests && sleep 10
	cd integration-tests && go test -v -coverprofile=../$(COVERAGE_DIR)/integration-tests-coverage.out ./... || (docker compose down && exit 1)
	cd integration-tests && docker compose down
	go tool cover -html=$(COVERAGE_DIR)/integration-tests-coverage.out -o $(COVERAGE_DIR)/integration-tests-coverage.html
	@echo "Integration test coverage report: $(COVERAGE_DIR)/integration-tests-coverage.html"
else
	@echo "Running integration tests..."
	cd integration-tests && docker compose up -d
	cd integration-tests && sleep 10
	cd integration-tests && go test -v ./... 2>&1 | tee ../$(BUILD_DIR)/integration-test-output.txt || (docker compose down && exit 1)
	cd integration-tests && docker compose down
	@echo "Integration test report: $(BUILD_DIR)/integration-test-output.txt"
endif

# All tests
.PHONY: test-all
test-all: test test-integ

# Clean target - remove build directory
.PHONY: clean
clean:
	@echo "Cleaning build directory..."
	rm -rf $(BUILD_DIR)
	@echo "Clean complete."

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build          - Compile/validate module and create package artifacts"
	@echo "  test           - Run unit and component tests (excludes integration tests)"
	@echo "  test-integ     - Run integration tests with Docker"
	@echo "  test-all       - Run all tests (unit, component, and integration)"
	@echo "  cover-test     - Run unit and component tests with coverage"
	@echo "  cover-test-integ - Run integration tests with coverage"
	@echo "  cover-test-all - Run all tests with coverage"
	@echo "  clean          - Remove build directory"
	@echo "  help           - Show this help message"
