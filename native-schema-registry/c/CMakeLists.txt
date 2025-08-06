cmake_minimum_required(VERSION 3.19)
IF (APPLE)
    #Defaults to Apple Compile. Explicitly install and set to gcc.
    set(CMAKE_C_COMPILER gcc)
ENDIF()
project(native_schema_registry_c C)
set(CMAKE_C_STANDARD 99)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

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
include_directories("include")
include_directories(${LIB_NATIVE_SCHEMA_REGISTRY_PATH})
# --- Python Integration ---

add_subdirectory("src")
include (CTest)
enable_testing()
add_subdirectory("test")