set(module_name native_schema_registry_clang)

#Fix the include path in GraalVM generated header file.
execute_process(COMMAND sed -ie "s/<graal_isolate.h>/\"graal_isolate.h\"/" ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/libnativeschemaregistry.h)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -ftest-coverage -fprofile-arcs ${sanitizer_checks} -ggdb3 -O2 -Wall")

add_library(
        ${module_name} SHARED
        #TODO: Use file glob
        glue_schema_registry_schema.c
        glue_schema_registry_serializer.c)

target_link_libraries(
        ${module_name}
        ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/${LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME}
)

#set(CMAKE_SWIG_FLAGS)
#find_package(SWIG REQUIRED)
#include(UseSWIG)
#
#file(
#    GLOB GsrSerDeSrc
#    "swig/glue_schema_registry_serde.i"
#)

##Swig language specific config. TODO: Move to different files
#Begin Python
#set(PYTHON_PROJECT_DIR, CPy)
#TODO: Below Swig target will be built on the a Python manylinux Docker images
#set(PYTHON_INCLUDE_PATH "")
#
#swig_add_library(
#        GsrSerDe
#        TYPE SHARED
#        LANGUAGE python
#        OUTPUT_DIR CPy/GsrSerDe
#        SOURCES ${GsrSerDeSrc}
#)

#set_target_properties(
#        GsrSerDe
#        PROPERTIES
#        INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/clang/include/;${PYTHON_INCLUDE_PATH}"
#        SWIG_USE_TARGET_INCLUDE_DIRECTORIES TRUE
#)

#target_link_libraries(
#        GsrSerDe
#        PUBLIC
#        ${module_name}
#)
#End Python