#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "memory_allocator.h"
#include "glue_schema_registry_error.h"

static bool validate(const char *err_msg) {
    if (err_msg == NULL) {
        return false;
    }

    return true;
}

glue_schema_registry_error *new_glue_schema_registry_error(
        const char *err_msg,
        int err_code) {
    if (!validate(err_msg)) {
        log_warn("Error message cannot be null", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }
    glue_schema_registry_error *error = NULL;
    error = (glue_schema_registry_error *) aws_common_malloc(sizeof(glue_schema_registry_error));
    error->msg = strdup(err_msg);
    error->code = err_code;

    return error;
}

void delete_glue_schema_registry_error(glue_schema_registry_error *error) {
    if (error == NULL) {
        return;
    }

    if (error->msg != NULL) {
        free(error->msg);
        error->msg = NULL;
    }
    error->code = 0;

    aws_common_free(error);
}

void glue_schema_registry_error_get_msg(glue_schema_registry_error *error, char *dst, size_t len) {
    size_t err_msg_len = strlen(error->msg);
    size_t required_len = err_msg_len + 1;

    //Using strncpy as safer strlcpy is not cross-platform.
    //Fit the message into fix array. We are doing this as MSVC doesn't support non-constant arrays.
    if (len >= required_len) {
        strncpy(dst, error->msg, err_msg_len);
        dst[err_msg_len] = '\0';
    } else {
        //Truncate the message
        strncpy(dst, error->msg, len - 1);
        dst[len - 1] = '\0';
    }
}
int glue_schema_registry_error_get_code(glue_schema_registry_error *error)
{
    return error->code;
}
char *glue_schema_registry_error_get_msgs(glue_schema_registry_error *error)
{
    return error->msg;
}

//Create and set the error to the glue_schema_registry_error pointer holder.
void throw_error(glue_schema_registry_error **p_err, const char *msg, int code) {
    if (p_err == NULL) {
        return;
    }

    glue_schema_registry_error *err = new_glue_schema_registry_error(msg, code);
    *p_err = err;
}

glue_schema_registry_error ** new_glue_schema_registry_error_holder(void) {
    glue_schema_registry_error **p_err =
            (glue_schema_registry_error **) aws_common_malloc(sizeof(glue_schema_registry_error *));
    *p_err = NULL;
    return p_err;
}

void delete_glue_schema_registry_error_holder(glue_schema_registry_error **p_err) {
    if (p_err == NULL) {
        return;
    }

    delete_glue_schema_registry_error(*p_err);
    aws_common_free(p_err);
}
