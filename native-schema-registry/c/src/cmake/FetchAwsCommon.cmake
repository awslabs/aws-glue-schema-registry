include(FetchContent)

set(AWS_C_COMMON aws-c-common)

FetchContent_Declare(
        AWS_C_COMMON
        GIT_REPOSITORY https://github.com/awslabs/aws-c-common.git
        GIT_TAG        v0.7.4
        GIT_SHALLOW    1
)

FetchContent_MakeAvailable(AWS_C_COMMON)