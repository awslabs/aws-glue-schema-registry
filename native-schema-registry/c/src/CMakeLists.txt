include(cmake/FetchAwsCommon.cmake)

#TODO: Disable coverage / debug for production builds.
if (CMAKE_SYSTEM_NAME MATCHES "^(Linux)$")
        list(APPEND TEST_COVERAGE "-ftest-coverage -fprofile-arcs")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${TEST_COVERAGE} -g -O0 -Wall")
        set(CMAKE_C_CLANG_TIDY clang-tidy -checks=-*,readability-*,-readability-math-missing-parentheses)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lgcov")
endif()

add_library(${AWS_COMMON_MEMALLOC} SHARED memory_allocator.c)
set_target_properties(${AWS_COMMON_MEMALLOC} PROPERTIES
    BUILD_RPATH "$ORIGIN"
    INSTALL_RPATH "$ORIGIN"
)
target_link_libraries(${AWS_COMMON_MEMALLOC} PRIVATE ${AWS_C_COMMON})

#Adding modules in the build order.
add_library(
        ${DATA_TYPES_MODULE_NAME} SHARED
        glue_schema_registry_schema.c
        read_only_byte_array.c
        mutable_byte_array.c
        glue_schema_registry_error.c
)
set_target_properties(${DATA_TYPES_MODULE_NAME} PROPERTIES
    BUILD_RPATH "$ORIGIN"
    INSTALL_RPATH "$ORIGIN"
)
add_custom_command(
        TARGET ${DATA_TYPES_MODULE_NAME}
        POST_BUILD
        COMMENT "copying data types for maven project build"
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${DATA_TYPES_MODULE_NAME}> ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_LINKER_FILE:${DATA_TYPES_MODULE_NAME}> ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/
        COMMENT "building maven project, native image target for ${BUILD_TARGET}"
)

# Build target-specific Maven integration
if(BUILD_TARGET STREQUAL "glibc")
    message(STATUS "Building with glibc target using native-image-glibc Maven profile")
    add_custom_command(
        TARGET ${DATA_TYPES_MODULE_NAME}
        POST_BUILD
        COMMENT "Building GraalVM native image with glibc"
        COMMAND mvn install -P native-image-glibc WORKING_DIRECTORY ${MAVEN_PROJECT_DIR}/..
    )
elseif(BUILD_TARGET STREQUAL "musl")
    message(STATUS "Building with musl target using native-image-musl Maven profile")
    add_custom_command(
        TARGET ${DATA_TYPES_MODULE_NAME}
        POST_BUILD
        COMMENT "Building GraalVM native image with musl"
        COMMAND mvn install -P native-image-musl WORKING_DIRECTORY ${MAVEN_PROJECT_DIR}/..
    )
endif()

#Add reference to the GraalVM generated Native schema registry module.
#First, fix the include path in GraalVM generated header file.
execute_process(
        COMMAND sed -ie "s/<graal_isolate.h>/\"graal_isolate.h\"/" ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/libnativeschemaregistry.h
        COMMENT "fixed include path in GraalVM generated header file"
)
add_library(
        ${NATIVE_SCHEMA_REGISTRY_MODULE_NAME} SHARED
        IMPORTED
)
set_target_properties(
        ${NATIVE_SCHEMA_REGISTRY_MODULE_NAME}
        PROPERTIES
        IMPORTED_LOCATION "${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/${LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME}"
        IMPORTED_IMPLIB "${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/${LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_OBJ_NAME}"
        INTERFACE_INCLUDE_DIRECTORIES "${LIB_NATIVE_SCHEMA_REGISTRY_PATH}"
        IMPORTED_NO_SONAME TRUE
        BUILD_RPATH "$ORIGIN"
        INSTALL_RPATH "$ORIGIN"
)


add_library(
        ${SERDE_MODULE_NAME} SHARED
        glue_schema_registry_serializer.c
        glue_schema_registry_deserializer.c
)
set_target_properties(${SERDE_MODULE_NAME} PROPERTIES
    BUILD_RPATH "$ORIGIN"
    INSTALL_RPATH "$ORIGIN"
)

target_link_libraries(
        ${DATA_TYPES_MODULE_NAME}
        ${AWS_COMMON_MEMALLOC}
)


target_link_libraries(
        ${SERDE_MODULE_NAME}
        ${AWS_COMMON_MEMALLOC}
        ${DATA_TYPES_MODULE_NAME}
        ${NATIVE_SCHEMA_REGISTRY_MODULE_NAME}
)

##Swig compilation

set(CMAKE_SWIG_FLAGS -intgosize 64)
find_package(SWIG 4.0.0 REQUIRED)
include(UseSWIG)

file(
        GLOB GsrSerDeSrc
        "swig/glue_schema_registry_serde.i"
)

#No need to run CLang Tidy on Swig generated code.
unset(CMAKE_C_CLANG_TIDY)


####Go - Copy libraries and headers only (no SWIG generation)
set(GO_ROOT_PATH ${PROJECT_SOURCE_DIR}/../golang)
set(GO_LIB_PATH ${GO_ROOT_PATH}/lib)

# Copy shared libraries and headers to golang lib directory
add_custom_command(
        TARGET ${SERDE_MODULE_NAME}
        POST_BUILD
        COMMENT "copying shared libraries and headers for golang integration"
        COMMAND ${CMAKE_COMMAND} -E make_directory ${GO_LIB_PATH}
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${DATA_TYPES_MODULE_NAME}> ${GO_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${NATIVE_SCHEMA_REGISTRY_MODULE_NAME}> ${GO_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${AWS_COMMON_MEMALLOC}> ${GO_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${SERDE_MODULE_NAME}> ${GO_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E make_directory ${GO_LIB_PATH}/include
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${PROJECT_SOURCE_DIR}/include/ ${GO_LIB_PATH}/include/
)

if(CMAKE_C_COMPILER_ID MATCHES "GNU")
    add_compile_options(--coverage)
    link_libraries(gcov)
endif()
