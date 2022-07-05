#ifndef NATIVE_SCHEMA_REGISTRY_GLUE_SCHEMA_REGISTRY_ERROR_H
#define NATIVE_SCHEMA_REGISTRY_GLUE_SCHEMA_REGISTRY_ERROR_H

#include <stdio.h>

//Error codes are arbitrarily listed from 5000. No specific reason.
#define ERR_CODE_INVALID_STATE 5000
#define ERR_CODE_NULL_PARAMETERS 5001
#define ERR_CODE_GRAALVM_INIT_EXCEPTION 5002
#define ERR_CODE_GRAALVM_TEARDOWN_EXCEPTION 5003
#define ERR_CODE_INVALID_PARAMETERS 5004
#define ERR_CODE_RUNTIME_ERROR 5005

//TODO: Improve error reporting to respect logging levels.
#define log_warn(msg, code) fprintf(stderr, "WARN: %s, Code: %d\n", msg, code)

#define MAX_ERROR_MSG_LEN 10000

/** Defines the glue_schema_registry_error structure for holding error messages and codes
 * resulting from function executions.
 */
typedef struct glue_schema_registry_error {
    char * msg;
    int code;
} glue_schema_registry_error;

glue_schema_registry_error * new_glue_schema_registry_error(const char * err_msg, int err_code);

void delete_glue_schema_registry_error(glue_schema_registry_error *error);

//Copies the given error's msg into dst array trimming the size as necessary.
void glue_schema_registry_error_get_msg(glue_schema_registry_error *error, char *dst, size_t len);

/**
 * Creates an instance of glue_schema_registry_error and writes it to the given
 * glue_schema_registry_error pointer holder (*p_err). It is expected that *p_err
 * is initialized by caller.
 * @param p_err Initialized glue_schema_registry_error pointer holder.
 * @param msg Error message to write.
 * @param code Non-zero error code.
 */
void throw_error(glue_schema_registry_error **p_err, const char *msg, int code);

#endif //NATIVE_SCHEMA_REGISTRY_GLUE_SCHEMA_REGISTRY_ERROR_H
