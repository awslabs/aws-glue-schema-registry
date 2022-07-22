include(cmake/FetchAwsCommon.cmake)

#TODO: Disable coverage / debug for production builds.
if (CMAKE_SYSTEM_NAME MATCHES "^(Linux)$")
        list(APPEND TEST_COVERAGE "-ftest-coverage -fprofile-arcs")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${TEST_COVERAGE} -g -O0 -Wall")
        set(CMAKE_C_CLANG_TIDY clang-tidy -checks=-*,readability-*)
endif()

add_library(${AWS_COMMON_MEMALLOC} SHARED memory_allocator.c)
target_link_libraries(${AWS_COMMON_MEMALLOC} PRIVATE ${AWS_C_COMMON})

#Adding modules in the build order.
add_library(
        ${DATA_TYPES_MODULE_NAME} SHARED
        glue_schema_registry_schema.c
        read_only_byte_array.c
        mutable_byte_array.c
        glue_schema_registry_error.c
)
add_custom_command(
        TARGET ${DATA_TYPES_MODULE_NAME}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${DATA_TYPES_MODULE_NAME}> ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_LINKER_FILE:${DATA_TYPES_MODULE_NAME}> ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/
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
)

add_library(
        ${SERDE_MODULE_NAME} SHARED
        glue_schema_registry_serializer.c
        glue_schema_registry_deserializer.c
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

####Python
find_package(PythonLibs REQUIRED)
set(PYTHON_MODULE_NAME GsrSerDePyGen)
set(PYTHON_ROOT_PATH ${PROJECT_SOURCE_DIR}/../python/PyGsrSerDe)
set(PYTHON_SOURCE_PATH ${PYTHON_ROOT_PATH}/)
set(PYTHON_GEN_LIB_PATH ${PYTHON_ROOT_PATH})
swig_add_library(
        ${PYTHON_MODULE_NAME}
        TYPE SHARED
        LANGUAGE python
        OUTPUT_DIR "${PYTHON_SOURCE_PATH}"
        SOURCES ${GsrSerDeSrc}
)
list(APPEND PYTHON_INCLUDES
        "${PROJECT_SOURCE_DIR}/include/"
        #TODO: Path needs to be configurable per version and OS
        "/usr/include/python3.7m/")

target_include_directories(${PYTHON_MODULE_NAME} PUBLIC ${PYTHON_INCLUDES})
set_target_properties(
        ${PYTHON_MODULE_NAME}
        PROPERTIES
        SWIG_USE_TARGET_INCLUDE_DIRECTORIES TRUE
)

##TODO: Fix this during release. We should segregate Debug and Release
##Copying built libraries to PYTHON project
add_custom_command(
        TARGET ${PYTHON_MODULE_NAME}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${DATA_TYPES_MODULE_NAME}> ${PYTHON_GEN_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${NATIVE_SCHEMA_REGISTRY_MODULE_NAME}> ${PYTHON_GEN_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${AWS_COMMON_MEMALLOC}> ${PYTHON_GEN_LIB_PATH}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${SERDE_MODULE_NAME}> ${PYTHON_GEN_LIB_PATH}/
)
