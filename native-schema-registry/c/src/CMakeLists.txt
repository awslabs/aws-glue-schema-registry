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
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${DATA_TYPES_MODULE_NAME}> ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_LINKER_FILE:${DATA_TYPES_MODULE_NAME}> ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/
        COMMAND mvn clean install -P native-image WORKING_DIRECTORY ${MAVEN_PROJECT_DIR}/..
)

#Add reference to the GraalVM generated Native schema registry module.
#First, fix the include path in GraalVM generated header file.
execute_process(COMMAND sed -ie "s/<graal_isolate.h>/\"graal_isolate.h\"/" ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/libnativeschemaregistry.h)
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
set(CMAKE_SWIG_FLAGS)
find_package(SWIG REQUIRED)
include(UseSWIG)

file(
        GLOB GsrSerDeSrc
        "swig/glue_schema_registry_serde.i"
)

#No need to run CLang Tidy on Swig generated code.
unset(CMAKE_C_CLANG_TIDY)

#####CSharp
set(CSHARP_MODULE_NAME GsrSerDeCsGen)
set(CSHARP_ROOT_PATH ${PROJECT_SOURCE_DIR}/../csharp/AWSGsrSerDe)
set(CSHARP_SOURCE_PATH ${CSHARP_ROOT_PATH}/AWSGsrSerDe/)
set(CSHARP_GEN_LIB_PATH ${CSHARP_ROOT_PATH}/Libs/)
swig_add_library(
        ${CSHARP_MODULE_NAME}
        TYPE SHARED
        LANGUAGE csharp
        OUTPUT_DIR "${CSHARP_SOURCE_PATH}"
        SOURCES ${GsrSerDeSrc}
)
set_target_properties(
        ${CSHARP_MODULE_NAME}
        PROPERTIES
        INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/include/"
        SWIG_USE_TARGET_INCLUDE_DIRECTORIES TRUE
        BUILD_RPATH "$ORIGIN"
        INSTALL_RPATH "$ORIGIN"
)

swig_link_libraries(
        ${CSHARP_MODULE_NAME}
        PUBLIC
        ${DATA_TYPES_MODULE_NAME}
        ${SERDE_MODULE_NAME}
        ${AWS_COMMON_MEMALLOC}
        ${NATIVE_SCHEMA_REGISTRY_MODULE_NAME}
)
##TODO: Fix this during release. We should segregate Debug and Release
##Copying built libraries to CSharp project
add_custom_command(
        TARGET ${CSHARP_MODULE_NAME}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${CSHARP_MODULE_NAME}> ${CSHARP_GEN_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${DATA_TYPES_MODULE_NAME}> ${CSHARP_GEN_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${NATIVE_SCHEMA_REGISTRY_MODULE_NAME}> ${CSHARP_GEN_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${AWS_COMMON_MEMALLOC}> ${CSHARP_GEN_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${SERDE_MODULE_NAME}> ${CSHARP_GEN_LIB_PATH}/
)

####Go 
set(GO_MODULE_NAME GsrSerDeGoGen)
set(GO_ROOT_PATH ${PROJECT_SOURCE_DIR}/../golang)
set(GO_SOURCE_PATH ${GO_ROOT_PATH}/pkg/GsrSerDe)
set(GO_LIB_PATH ${GO_ROOT_PATH}/lib)
swig_add_library(
        ${GO_MODULE_NAME}
        TYPE SHARED
        LANGUAGE go
        OUTPUT_DIR "${GO_SOURCE_PATH}"
        SOURCES ${GsrSerDeSrc}
)
set_target_properties(
        ${GO_MODULE_NAME}
        PROPERTIES
        INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/include/"
        SWIG_USE_TARGET_INCLUDE_DIRECTORIES TRUE
        BUILD_RPATH "$ORIGIN"
        INSTALL_RPATH "$ORIGIN"
)
swig_link_libraries(
        ${GO_MODULE_NAME}
        PUBLIC
        ${DATA_TYPES_MODULE_NAME}
        ${SERDE_MODULE_NAME}
        ${AWS_COMMON_MEMALLOC}
        ${NATIVE_SCHEMA_REGISTRY_MODULE_NAME}
)
# Copy shared libraries to lib directory
add_custom_command(
        TARGET ${GO_MODULE_NAME}
        POST_BUILD
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
