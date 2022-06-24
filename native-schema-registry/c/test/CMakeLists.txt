list(APPEND sanitizer_checks "-fsanitize=address,undefined -fsanitize-recover=address -fno-stack-protector")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${sanitizer_checks} -fprofile-arcs -ftest-coverage -ggdb3 -O2 -Wall")
include(cmake/FetchCMocka.cmake)

list(
        APPEND
        tests
        #Add tests below:
        glue_schema_registry_schema_test
        read_only_byte_array_test
        mutable_byte_array_test
)

foreach (test ${tests}) 
    add_executable(
            "${test}"
            "${test}.c"
            )
    target_link_libraries(
            "${test}"
            "${DATA_TYPES_MODULE_NAME}"
            "cmocka"
    )

    add_test(NAME "${test}" COMMAND "${test}" ${CMAKE_CURRENT_BINARY_DIR})
endforeach ()

add_custom_target(copy-libs-for-tests ALL
   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${DATA_TYPES_MODULE_NAME}> ${CMAKE_CURRENT_BINARY_DIR}/
   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:cmocka> ${CMAKE_CURRENT_BINARY_DIR}/
)