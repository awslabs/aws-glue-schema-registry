#include <stdio.h>

//TODO: Exception handling and error reporting
#define ERR_CODE_INVALID_STATE 5000
#define ERR_CODE_NULL_PARAMETERS 5001
#define ERR_CODE_GRAALVM_INIT_EXCEPTION 5002
#define ERR_CODE_GRAALVM_TEARDOWN_EXCEPTION 5003
#define ERR_CODE_INVALID_PARAMETERS 5004

#define log_error(msg, code) fprintf(stderr, "ERROR: %s, Code: %d\n", msg, code)
