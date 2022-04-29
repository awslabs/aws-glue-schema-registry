set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${sanitizer_checks} -fsanitize-recover=address -fno-stack-protector -ggdb3 -O2 -Wall")

include(cmake/FetchCMocka.cmake)

list(
        APPEND
        tests
        #Add tests below:
        glue_schema_registry_schema_test
)

foreach (test ${tests})
    add_executable(
            "${test}"
            "${test}.c"
    )
    target_link_libraries(
            "${test}"
            native_schema_registry_clang
            ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/${LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME}
            cmocka-static
    )
    add_test(NAME "${test}" COMMAND "${test}")
endforeach ()
