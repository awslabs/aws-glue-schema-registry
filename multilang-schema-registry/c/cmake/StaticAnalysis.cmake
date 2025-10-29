# Run static analysis with clang-tidy
message(STATUS "=== Running Static Analysis ===")

# Find all C source files
file(GLOB_RECURSE C_FILES "${CMAKE_SOURCE_DIR}/src/*.c")

# Run clang-tidy on each file
foreach(C_FILE ${C_FILES})
    execute_process(
        COMMAND clang-tidy ${C_FILE} -- 
            -I ${CMAKE_SOURCE_DIR}/include 
            -I ${CMAKE_SOURCE_DIR}/../target 
            -I ${CMAKE_BINARY_DIR}/_deps/aws_c_common-src/include
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE CLANG_OUTPUT
        ERROR_VARIABLE CLANG_ERROR
        RESULT_VARIABLE CLANG_RESULT
    )
    
    # Filter out warnings from external headers
    string(REGEX REPLACE ".*graal_isolate\\.h.*\n?" "" FILTERED_OUTPUT "${CLANG_OUTPUT}")
    string(REGEX REPLACE ".*libnativeschemaregistry\\.h.*\n?" "" FILTERED_OUTPUT "${FILTERED_OUTPUT}")
    string(REGEX REPLACE ".*graal_isolate\\.h.*\n?" "" FILTERED_ERROR "${CLANG_ERROR}")
    string(REGEX REPLACE ".*libnativeschemaregistry\\.h.*\n?" "" FILTERED_ERROR "${FILTERED_ERROR}")
    
    # Check for errors or warnings
    if(FILTERED_OUTPUT MATCHES "error:|warning:" OR FILTERED_ERROR MATCHES "error:|warning:")
        message(FATAL_ERROR "Static analysis failed:\n${FILTERED_OUTPUT}\n${FILTERED_ERROR}")
    endif()
endforeach()

message(STATUS "✓ Static analysis passed")
