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

add_library(mock_foo SHARED mock_foo.c)

foreach (test ${tests})
    add_executable(
            "${test}"
            "${test}.c"
            )
    target_link_libraries(
            "${test}"
            native_schema_registry_c
            ${LIB_NATIVE_SCHEMA_REGISTRY_PATH}/${LIB_NATIVE_SCHEMA_REGISTRY_LIBRARY_NAME}
            cmocka
    )

    add_test(NAME "${test}" COMMAND "${test}")
endforeach ()