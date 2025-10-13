cmake_minimum_required(VERSION 3.19)
IF (APPLE)
    #Defaults to Apple Compile. Explicitly install and set to gcc.
    set(CMAKE_C_COMPILER gcc)
ENDIF()
project(native_schema_registry_c C)
set(CMAKE_C_STANDARD 99)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Build target configuration
set(BUILD_TARGET "glibc" CACHE STRING "Build target: glibc (default), musl, or musl-cross")
set_property(CACHE BUILD_TARGET PROPERTY STRINGS "glibc" "musl" "musl-cross")

# Validate build target
if(NOT BUILD_TARGET MATCHES "^(glibc|musl|musl-cross)$")
    message(FATAL_ERROR 
        "Invalid BUILD_TARGET: ${BUILD_TARGET}\n"
        "Valid options are:\n"
        "  glibc      - Standard glibc build (default)\n"
        "  musl       - Native musl build\n"
        "  musl-cross - Cross-compile musl on glibc system using Docker"
    )
endif()

message(STATUS "Build target: ${BUILD_TARGET}")

set(MAVEN_PROJECT_DIR "${PROJECT_SOURCE_DIR}/../target")
##Global variables
#Path to GraalVM generated shared library
set(LIB_NATIVE_SCHEMA_REGISTRY_PATH "${PROJECT_SOURCE_DIR}/../target")
set(LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME_PREFIX libnativeschemaregistry)
##OS Specific variables
IF (WIN32)
    set(LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME ${LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME_PREFIX}.dll)
    set(LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_OBJ_NAME ${LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME_PREFIX}.lib)
ELSEIF(APPLE)
    set(LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME ${LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME_PREFIX}.dylib)
ELSE()
    set(LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME ${LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME_PREFIX}.so)
ENDIF()
set(DATA_TYPES_MODULE_NAME native_schema_registry_c_data_types)
set(SERDE_MODULE_NAME native_schema_registry_c)
set(AWS_COMMON_MEMALLOC aws_common_memalloc)
set(NATIVE_SCHEMA_REGISTRY_MODULE_NAME libnativeschemaregistry)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)

##Global RPATH Configuration for Portable Libraries
set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_INSTALL_RPATH "$ORIGIN")

include_directories("include")
include_directories(${LIB_NATIVE_SCHEMA_REGISTRY_PATH})
# --- Python Integration ---

add_subdirectory("src")
include (CTest)
enable_testing()
add_subdirectory("test")

# Option to disable quality gates (they run by default)
option(DISABLE_QUALITY_GATES "Disable automatic quality gates on build" OFF)

# Define test targets that need to be built
set(TEST_TARGETS 
    glue_schema_registry_schema_test
    read_only_byte_array_test
    mutable_byte_array_test
    glue_schema_registry_error_test
    glue_schema_registry_serializer_test
    glue_schema_registry_deserializer_test
)

# Manual quality gates target (always available)
add_custom_target(quality_gates
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/RunTests.cmake
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/StaticAnalysis.cmake
    COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/CoverageCheck.cmake
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Running quality gates: tests, static analysis, and coverage"
    VERBATIM
)

# Add dependencies to all main targets and tests
add_dependencies(quality_gates ${SERDE_MODULE_NAME} ${DATA_TYPES_MODULE_NAME} ${TEST_TARGETS} copy-libs-for-tests)

if(NOT DISABLE_QUALITY_GATES)
    # Add quality gates to run automatically on every build
    add_custom_target(quality_gates_auto ALL
        COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/RunTests.cmake
        COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/StaticAnalysis.cmake
        COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/CoverageCheck.cmake
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running quality gates: tests, static analysis, and coverage"
        VERBATIM
    )
    add_dependencies(quality_gates_auto ${SERDE_MODULE_NAME} ${DATA_TYPES_MODULE_NAME} ${TEST_TARGETS} copy-libs-for-tests)
endif()
