set(DATA_TYPES_MODULE_NAME native_schema_registry_c_data_types)
set(SERDE_MODULE_NAME native_schema_registry_c)
set(NATIVE_SCHEMA_REGISTRY_MODULE_NAME libnativeschemaregistry)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)

#Fix the include path in GraalVM generated header file.
execute_process(COMMAND sed -ie "s/<graal_isolate.h>/\"graal_isolate.h\"/" ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/libnativeschemaregistry.h)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -ftest-coverage -fprofile-arcs -ggdb3 -O2 -Wall")

#Adding modules in the build order.
add_library(
        ${DATA_TYPES_MODULE_NAME} SHARED
        glue_schema_registry_schema.c
        read_only_byte_array.c
        mutable_byte_array.c
)

#Add reference to the GraalVM generated Native schema registry module.
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
        ${SERDE_MODULE_NAME}
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

#####CSharp
set(CSHARP_MODULE_NAME GsrSerDeCsGen)
set(CSHARP_SOURCE_PATH ${PROJECT_SOURCE_DIR}/../csharp/AWSGsrSerDe/AWSGsrSerDe/)
set(CSHARP_GEN_LIB_PATH ${CSHARP_SOURCE_PATH}/bin/)

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
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${SERDE_MODULE_NAME}> ${CSHARP_GEN_LIB_PATH}/
)

