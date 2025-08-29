# Variables
BUILD_DIR := build
COVERAGE_DIR := $(BUILD_DIR)/coverage
PKG_DIR := $(BUILD_DIR)/pkg


# Proto generation variables
PROTO_SRC_DIR := ../shared/test/protos
PROTO_OUT_DIR := integration-tests/testpb
GO_MODULE_PATH := github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang
GO_MODULE_PREFIX := github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb
PACKAGE_PREFIX_TO_TRIM := com.amazonaws.services.schemaregistry.tests.protobuf
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
test-integ: setup-dirs generate-protos
ifdef COVERAGE
	@echo "Running integration tests with coverage..."
	cd integration-tests && docker compose up -d 
	cd integration-tests && go test -v -coverpkg=../pkg/gsrserde-go/...,../pkg/GsrSerDe/... -coverprofile=../$(COVERAGE_DIR)/integration-tests-coverage.out ./...
	cd integration-tests && docker compose down
	cd integration-tests && go tool cover -html=../$(COVERAGE_DIR)/integration-tests-coverage.out -o ../$(COVERAGE_DIR)/integration-tests-coverage.html
	@echo "Integration test coverage report: $(COVERAGE_DIR)/integration-tests-coverage.html"
else
	@echo "Running integration tests..."
	cd integration-tests && docker compose up -d | tee  ../$(BUILD_DIR)/integ-docker-logs.txt
	cd integration-tests && stdbuf -oL go test -v -count=1 ./... 2>&1 | tee ../$(BUILD_DIR)/integration-test-output.txt
	cd integration-tests && docker compose logs > docker-logs.txt
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

# Generate protobuf files target
.PHONY: generate-protos
generate-protos: setup-dirs
	@echo "Generating protobuf files..."
	@echo "Proto source: $(PROTO_SRC_DIR)"
	@echo "Output directory: $(PROTO_OUT_DIR)"
	@echo "Package prefix to trim: $(PACKAGE_PREFIX_TO_TRIM)"
	@mkdir -p $(PROTO_OUT_DIR)
	@for proto_file in $(PROTO_SRC_DIR)/*.proto; do \
		if [ -f "$$proto_file" ]; then \
			basename=$$(basename "$$proto_file"); \
			package_name=$$(awk '/^\s*package\s+[^;]+;/ { print $$2 }' "$$proto_file" | tr -d ';'); \
			has_go_package=$$(grep -q "option go_package" "$$proto_file" && echo "true" || echo "false"); \
			echo "Processing $$basename: package=$$package_name, has_go_package=$$has_go_package"; \
			if [ "$$has_go_package" = "false" ]; then \
				trimmed_package=$$(echo "$$package_name" | sed "s/^$(PACKAGE_PREFIX_TO_TRIM)\.//"); \
				package_path=$$(echo "$$trimmed_package" | sed 's/\./\//g'); \
				output_dir="$(PROTO_OUT_DIR)/$$package_path"; \
				import_path="$(GO_MODULE_PREFIX)/$$package_path"; \
				echo "  -> Trimmed package: $$trimmed_package"; \
				echo "  -> Package path: $$package_path"; \
				echo "  -> Output directory: $$output_dir"; \
				echo "  -> Import path: $$import_path"; \
				mkdir -p "$$output_dir"; \
				protoc --proto_path=$(PROTO_SRC_DIR) \
					--go_out="$$output_dir" \
					--go_opt=paths=source_relative \
					--go_opt=M$$basename="$$import_path" \
					"$$proto_file"; \
			else \
				echo "  -> Using existing go_package option"; \
				protoc --proto_path=$(PROTO_SRC_DIR) \
					--go_out=$(PROTO_OUT_DIR) \
					--go_opt=paths=source_relative \
					"$$proto_file"; \
			fi; \
		fi; \
	done
	@echo "Protobuf generation complete"

# Allow overriding variables for custom proto generation
.PHONY: generate-protos-custom
generate-protos-custom:
	$(MAKE) generate-protos \
		PROTO_SRC_DIR=$(or $(CUSTOM_PROTO_SRC_DIR),$(PROTO_SRC_DIR)) \
		PROTO_OUT_DIR=$(or $(CUSTOM_PROTO_OUT_DIR),$(PROTO_OUT_DIR)) \
		PACKAGE_PREFIX_TO_TRIM=$(or $(CUSTOM_PACKAGE_PREFIX),$(PACKAGE_PREFIX_TO_TRIM))

# Clean protobuf generated files
.PHONY: clean-protos
clean-protos:
	@echo "Cleaning generated protobuf files..."
	@rm -rf $(PROTO_OUT_DIR)
	@echo "Protobuf files cleaned"

# Alpine-based integration testing (new alternative)
.PHONY: test-integ-dockerized
test-integ-dockerized:  setup-dirs 
ifdef COVERAGE
	@echo "Running dockerized integration tests with coverage..."
	cd integration-tests && COVERAGE=true docker compose -f docker-compose.dockerized.yml up --build --abort-on-container-exit
	cd integration-tests && docker compose -f docker-compose.dockerized.yml down
	@echo "Alpine integration test coverage report: $(COVERAGE_DIR)/dockerized-integration-coverage.html"
else
	@echo "Running Alpine integration tests..."
	cd integration-tests && docker compose -f docker-compose.dockerized.yml up --build --abort-on-container-exit | tee ../$(BUILD_DIR)/dockerized-integ-docker-logs.txt
	cd integration-tests && docker compose -f docker-compose.dockerized.yml logs golang-tests > ../$(BUILD_DIR)/dockerized-test-logs.txt 2>&1 || true
	cd integration-tests && docker compose -f docker-compose.dockerized.yml down
	@echo "Alpine integration test report: $(BUILD_DIR)/dockerized-integration-test-output.txt"
endif

# Alpine coverage wrapper
.PHONY: cover-test-integ-dockerized
cover-test-integ-dockerized: setup-dirs
	@echo "Running Alpine integration tests with coverage..."
	$(MAKE) test-integ-dockerized COVERAGE=true

# Build Alpine test image only
.PHONY: build-dockerized-test-image
build-dockerized-test-image: setup-dirs generate-protos
	@echo "Building Alpine test image..."
	docker build -f integration-tests/Dockerfile.dockerized-tests -t golang-gsr-tests:dockerized ..

# Clean Alpine containers and images
.PHONY: clean-dockerized
clean-dockerized:
	@echo "Cleaning Alpine test containers and images..."
	cd integration-tests && docker compose -f docker-compose.dockerized.yml down --remove-orphans || true
	docker rmi golang-gsr-tests:dockerized || true

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build          - Compile/validate module and create package artifacts"
	@echo "  test           - Run unit and component tests (excludes integration tests)"
	@echo "  test-integ     - Run integration tests with Docker"
	@echo "  test-integ-dockerized - Run integration tests in Alpine container"
	@echo "  test-all       - Run all tests (unit, component, and integration)"
	@echo "  cover-test     - Run unit and component tests with coverage"
	@echo "  cover-test-integ - Run integration tests with coverage"
	@echo "  cover-test-integ-dockerized - Run Alpine integration tests with coverage"
	@echo "  cover-test-all - Run all tests with coverage"
	@echo "  generate-protos - Generate .pb.go files from .proto files"
	@echo "  generate-protos-custom - Generate protos with custom variables"
	@echo "  build-dockerized-test-image - Build Alpine test image only"
	@echo "  clean-protos   - Remove generated .pb.go files"
	@echo "  clean-dockerized   - Clean Alpine containers and images"
	@echo "  clean          - Remove build directory"
	@echo "  help           - Show this help message"
# Variables
BUILD_DIR := build
COVERAGE_DIR := $(BUILD_DIR)/coverage
PKG_DIR := $(BUILD_DIR)/pkg

# Proto generation variables
PROTO_SRC_DIR := ../shared/test/protos
PROTO_OUT_DIR := integration-tests/testpb
GO_MODULE_PATH := github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang
GO_MODULE_PREFIX := github.com/awslabs/aws-glue-schema-registry/native-schema-registry/golang/integration-tests/testpb
PACKAGE_PREFIX_TO_TRIM := com.amazonaws.services.schemaregistry.tests.protobuf

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
	go test -v --count=1 -coverprofile=$(COVERAGE_DIR)/unit-coverage.out $(shell go list ./... | grep -v integration-tests)
	go tool cover -html=$(COVERAGE_DIR)/unit-coverage.out -o $(COVERAGE_DIR)/unit-coverage.html
	@echo "Unit and component test coverage report: $(COVERAGE_DIR)/unit-coverage.html"
else
	@echo "Running unit and component tests..."
	go test -v -count=1 $(shell go list ./... | grep -v integration-tests) 2>&1 | tee $(BUILD_DIR)/unit-test-output.txt
endif

# Integration tests with Docker (integration-tests/ directory only)
.PHONY: test-integ
test-integ: setup-dirs generate-protos
ifdef COVERAGE
	@echo "Running integration tests with coverage..."
	cd integration-tests && docker compose up -d 
	cd integration-tests && go test -v -coverpkg=../pkg/gsrserde-go/...,../pkg/GsrSerDe/... -coverprofile=../$(COVERAGE_DIR)/integration-tests-coverage.out ./...
	cd integration-tests && docker compose down
	cd integration-tests && go tool cover -html=../$(COVERAGE_DIR)/integration-tests-coverage.out -o ../$(COVERAGE_DIR)/integration-tests-coverage.html
	@echo "Integration test coverage report: $(COVERAGE_DIR)/integration-tests-coverage.html"
else
	@echo "Running integration tests..."
	cd integration-tests && docker compose up -d | tee  ../$(BUILD_DIR)/integ-docker-logs.txt
	cd integration-tests && stdbuf -oL go test -v -count=1 ./... 2>&1 | tee ../$(BUILD_DIR)/integration-test-output.txt
	cd integration-tests && docker compose logs > docker-logs.txt
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

# Generate protobuf files target
.PHONY: generate-protos
generate-protos: setup-dirs
	@echo "Generating protobuf files..."
	@echo "Proto source: $(PROTO_SRC_DIR)"
	@echo "Output directory: $(PROTO_OUT_DIR)"
	@echo "Package prefix to trim: $(PACKAGE_PREFIX_TO_TRIM)"
	@mkdir -p $(PROTO_OUT_DIR)
	@for proto_file in $(PROTO_SRC_DIR)/*.proto; do \
		if [ -f "$$proto_file" ]; then \
			basename=$$(basename "$$proto_file"); \
			package_name=$$(awk '/^\s*package\s+[^;]+;/ { print $$2 }' "$$proto_file" | tr -d ';'); \
			has_go_package=$$(grep -q "option go_package" "$$proto_file" && echo "true" || echo "false"); \
			echo "Processing $$basename: package=$$package_name, has_go_package=$$has_go_package"; \
			if [ "$$has_go_package" = "false" ]; then \
				trimmed_package=$$(echo "$$package_name" | sed "s/^$(PACKAGE_PREFIX_TO_TRIM)\.//"); \
				package_path=$$(echo "$$trimmed_package" | sed 's/\./\//g'); \
				output_dir="$(PROTO_OUT_DIR)/$$package_path"; \
				import_path="$(GO_MODULE_PREFIX)/$$package_path"; \
				echo "  -> Trimmed package: $$trimmed_package"; \
				echo "  -> Package path: $$package_path"; \
				echo "  -> Output directory: $$output_dir"; \
				echo "  -> Import path: $$import_path"; \
				mkdir -p "$$output_dir"; \
				protoc --proto_path=$(PROTO_SRC_DIR) \
					--go_out="$$output_dir" \
					--go_opt=paths=source_relative \
					--go_opt=M$$basename="$$import_path" \
					"$$proto_file"; \
			else \
				echo "  -> Using existing go_package option"; \
				protoc --proto_path=$(PROTO_SRC_DIR) \
					--go_out=$(PROTO_OUT_DIR) \
					--go_opt=paths=source_relative \
					"$$proto_file"; \
			fi; \
		fi; \
	done
	@echo "Protobuf generation complete"

# Allow overriding variables for custom proto generation
.PHONY: generate-protos-custom
generate-protos-custom:
	$(MAKE) generate-protos \
		PROTO_SRC_DIR=$(or $(CUSTOM_PROTO_SRC_DIR),$(PROTO_SRC_DIR)) \
		PROTO_OUT_DIR=$(or $(CUSTOM_PROTO_OUT_DIR),$(PROTO_OUT_DIR)) \
		PACKAGE_PREFIX_TO_TRIM=$(or $(CUSTOM_PACKAGE_PREFIX),$(PACKAGE_PREFIX_TO_TRIM))

# Clean protobuf generated files
.PHONY: clean-protos
clean-protos:
	@echo "Cleaning generated protobuf files..."
	@rm -rf $(PROTO_OUT_DIR)
	@echo "Protobuf files cleaned"

# Alpine-based integration testing (new alternative)
.PHONY: test-integ-dockerized
test-integ-dockerized:  setup-dirs 
ifdef COVERAGE
	@echo "Running dockerized integration tests with coverage..."
	cd integration-tests && COVERAGE=true docker compose -f docker-compose.dockerized.yml up --build --abort-on-container-exit
	cd integration-tests && docker compose -f docker-compose.dockerized.yml down
	@echo "Alpine integration test coverage report: $(COVERAGE_DIR)/dockerized-integration-coverage.html"
else
	@echo "Running Alpine integration tests..."
	cd integration-tests && docker compose -f docker-compose.dockerized.yml up --build --abort-on-container-exit | tee ../$(BUILD_DIR)/dockerized-integ-docker-logs.txt
	cd integration-tests && docker compose -f docker-compose.dockerized.yml logs golang-tests > ../$(BUILD_DIR)/dockerized-test-logs.txt 2>&1 || true
	cd integration-tests && docker compose -f docker-compose.dockerized.yml down
	@echo "Alpine integration test report: $(BUILD_DIR)/dockerized-integration-test-output.txt"
endif

# Alpine coverage wrapper
.PHONY: cover-test-integ-dockerized
cover-test-integ-dockerized: setup-dirs
	@echo "Running Alpine integration tests with coverage..."
	$(MAKE) test-integ-dockerized COVERAGE=true

# Build Alpine test image only
.PHONY: build-dockerized-test-image
build-dockerized-test-image: setup-dirs generate-protos
	@echo "Building Alpine test image..."
	docker build -f integration-tests/Dockerfile.dockerized-tests -t golang-gsr-tests:dockerized ..

# Clean Alpine containers and images
.PHONY: clean-dockerized
clean-dockerized:
	@echo "Cleaning Alpine test containers and images..."
	cd integration-tests && docker compose -f docker-compose.dockerized.yml down --remove-orphans || true
	docker rmi golang-gsr-tests:dockerized || true

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build          - Compile/validate module and create package artifacts"
	@echo "  test           - Run unit and component tests (excludes integration tests)"
	@echo "  test-integ     - Run integration tests with Docker"
	@echo "  test-integ-dockerized - Run integration tests in Alpine container"
	@echo "  test-all       - Run all tests (unit, component, and integration)"
	@echo "  cover-test     - Run unit and component tests with coverage"
	@echo "  cover-test-integ - Run integration tests with coverage"
	@echo "  cover-test-integ-dockerized - Run Alpine integration tests with coverage"
	@echo "  cover-test-all - Run all tests with coverage"
	@echo "  generate-protos - Generate .pb.go files from .proto files"
	@echo "  generate-protos-custom - Generate protos with custom variables"
	@echo "  build-dockerized-test-image - Build Alpine test image only"
	@echo "  clean-protos   - Remove generated .pb.go files"
	@echo "  clean-dockerized   - Clean Alpine containers and images"
	@echo "  clean          - Remove build directory"
	@echo "  help           - Show this help message"
