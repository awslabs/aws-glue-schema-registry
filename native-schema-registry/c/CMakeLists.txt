cmake_minimum_required(VERSION 3.19)
IF (APPLE)
    #Defaults to Apple Compile. Explicitly install and set to gcc.
    set(CMAKE_C_COMPILER gcc)
ENDIF()
project(native_schema_registry_c C)
set(CMAKE_C_STANDARD 99)

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

add_subdirectory("src")
include (CTest)
enable_testing()
add_subdirectory("test")
include_directories("include")