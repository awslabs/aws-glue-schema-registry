if (CMAKE_SYSTEM_NAME MATCHES "^(Linux|Darwin)$")
    list(APPEND SANITIZER_CHECKS "-fsanitize=address,undefined -fsanitize-recover=address -fno-stack-protector")
else()
    #TODO: Figure out equivalent options for Windows
    list(APPEND SANITIZER_CHECKS "")
endif()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${SANITIZER_CHECKS} -Wall")
include(cmake/FetchCMocka.cmake)

set(TEST_HELPERS test_helpers)
add_library(${TEST_HELPERS} glue_schema_registry_test_helper.c)
target_link_libraries(${TEST_HELPERS} cmocka)

##GraalVM Mock
add_library(${NATIVE_SCHEMA_REGISTRY_MODULE_NAME} libnativeschemaregistry_mock.c)
target_link_libraries(${NATIVE_SCHEMA_REGISTRY_MODULE_NAME} cmocka)

list(
        APPEND
        tests
        #Add tests below:
        glue_schema_registry_schema_test
        read_only_byte_array_test
        mutable_byte_array_test
        glue_schema_registry_error_test
)

#TODO: These tests don't work on Windows and OSX due to CMake not linking the mock library.
#We have to investigate and fix these.
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND
            tests
            glue_schema_registry_serializer_test
            glue_schema_registry_deserializer_test
    )
ENDIF ()

foreach (test ${tests})
    add_executable(
            "${test}"
            "${test}.c"
    )
    target_link_libraries(
            "${test}"
            "${DATA_TYPES_MODULE_NAME}"
            "${SERDE_MODULE_NAME}"
            "${NATIVE_SCHEMA_REGISTRY_MODULE_NAME}"
            "${AWS_COMMON_MEMALLOC}"
            "${TEST_HELPERS}"
            "cmocka"
    )

    add_test(NAME "${test}" COMMAND "${test}" ${CMAKE_CURRENT_BINARY_DIR})
endforeach ()

add_custom_target(copy-libs-for-tests ALL
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${DATA_TYPES_MODULE_NAME}> ${CMAKE_CURRENT_BINARY_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${SERDE_MODULE_NAME}> ${CMAKE_CURRENT_BINARY_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:cmocka> ${CMAKE_CURRENT_BINARY_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${AWS_COMMON_MEMALLOC}> ${CMAKE_CURRENT_BINARY_DIR}/
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${NATIVE_SCHEMA_REGISTRY_MODULE_NAME}> ${CMAKE_CURRENT_BINARY_DIR}/
)

#Create code coverage target
if (CMAKE_SYSTEM_NAME MATCHES "^(Linux)$")
    set(CMAKE_COMPILER_IS_GNUCXX 1)
    include(cmake/CodeCoverage.cmake)
    set(min_code_coverage 100)
    setup_target_for_coverage(coverage "ctest ." coverage ${min_code_coverage})
endif()