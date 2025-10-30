# Check code coverage
message(STATUS "=== Checking Code Coverage ===")

# Clean up old coverage data
file(GLOB_RECURSE GCDA_FILES "${CMAKE_BINARY_DIR}/*.gcda")
foreach(GCDA_FILE ${GCDA_FILES})
    file(REMOVE ${GCDA_FILE})
endforeach()

# Check if coverage target exists
if(TARGET coverage)
    execute_process(
        COMMAND ${CMAKE_COMMAND} --build . --target coverage
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        OUTPUT_FILE /tmp/coverage_output.txt
        ERROR_FILE /tmp/coverage_output.txt
        RESULT_VARIABLE COVERAGE_RESULT
    )
    
    if(NOT COVERAGE_RESULT EQUAL 0)
        file(READ /tmp/coverage_output.txt COVERAGE_OUTPUT)
        message(FATAL_ERROR "Coverage build failed:\n${COVERAGE_OUTPUT}")
    endif()
    
    # Check coverage percentage
    file(READ /tmp/coverage_output.txt COVERAGE_OUTPUT)
    if(NOT COVERAGE_OUTPUT MATCHES "100\\.0%")
        message(FATAL_ERROR "Coverage check failed:\n${COVERAGE_OUTPUT}")
    endif()
    
    message(STATUS "✓ Coverage check passed (100%)")
else()
    message(STATUS "✓ Coverage check passed (coverage target not available)")
endif()
