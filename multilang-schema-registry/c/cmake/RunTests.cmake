# Run tests with proper library path
message(STATUS "=== Running Tests ===")

# Set library path for tests
set(ENV{LD_LIBRARY_PATH} "${CMAKE_SOURCE_DIR}/../../target:${CMAKE_BINARY_DIR}/src:${CMAKE_BINARY_DIR}/test:${CMAKE_BINARY_DIR}/_deps/cmocka-build/src:$ENV{LD_LIBRARY_PATH}")

execute_process(
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    RESULT_VARIABLE TEST_RESULT
)

if(NOT TEST_RESULT EQUAL 0)
    message(FATAL_ERROR "Tests failed")
endif()

message(STATUS "✓ All tests passed")
