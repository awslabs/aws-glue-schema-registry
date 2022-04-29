cmake_minimum_required(VERSION 3.19)
project(native_schema_registry_clang C)
set(CMAKE_C_STANDARD 99)

##Global variables
#Path to GraalVM generated shared library
set(LIB_NATIVE_SCHEMA_REGISTRY_PATH "${PROJECT_SOURCE_DIR}/../target")
list(APPEND sanitizer_checks "-fsanitize=address,undefined")

##OS Specific variables
IF (WIN32)
    set(LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME libnativeschemaregistry.dll)
ELSEIF(APPLE)
    set(LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME libnativeschemaregistry.dylib)
ELSE()
    set(LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME libnativeschemaregistry.so)
ENDIF()

add_subdirectory("src")
include (CTest)
enable_testing()
add_subdirectory("test")
include_directories("include")