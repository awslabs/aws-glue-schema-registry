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

# Test target - run tests with terminal output + JUnit XML report
.PHONY: test
test: setup-dirs
	@echo "Running tests..."
	go test -v ./... > $(BUILD_DIR)/test-output.txt 2>&1
	cat $(BUILD_DIR)/test-output.txt
	cat $(BUILD_DIR)/test-output.txt | go-junit-report > $(BUILD_DIR)/junit-report.xml
	@echo "Test report generated: $(BUILD_DIR)/junit-report.xml"


# Coverage target - run tests with coverage reporting
.PHONY: coverage
coverage: setup-dirs
	@echo "Running tests with coverage..."
	go test -v -coverprofile=$(COVERAGE_DIR)/coverage.out ./...
	go tool cover -html=$(COVERAGE_DIR)/coverage.out -o $(COVERAGE_DIR)/coverage.html
	@echo "Coverage report generated:"
	@echo "  - Profile: $(COVERAGE_DIR)/coverage.out"
	@echo "  - HTML: $(COVERAGE_DIR)/coverage.html"

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
	@echo "  build     - Compile/validate module and create package artifacts"
	@echo "  test      - Run tests with terminal output and JUnit XML report"
	@echo "  coverage  - Run tests with coverage and generate HTML report"
	@echo "  clean     - Remove build directory"
	@echo "  help      - Show this help message"
