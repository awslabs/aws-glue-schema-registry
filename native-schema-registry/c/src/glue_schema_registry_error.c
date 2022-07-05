#include <stdlib.h>
#include <string.h>
#include "../include/glue_schema_registry_error.h"

static int validate(const char *err_msg) {
    if (err_msg == NULL) {
        return 1;
    }

    return 0;
}

glue_schema_registry_error *new_glue_schema_registry_error(
        const char *err_msg,
        int err_code) {
    if (validate(err_msg) != 0) {
        log_warn("Error message cannot be null", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }
    glue_schema_registry_error *error = NULL;
    error = (glue_schema_registry_error *) malloc(sizeof(glue_schema_registry_error));
    error->msg = strdup(err_msg);
    error->code = err_code;

    return error;
}

void delete_glue_schema_registry_error(glue_schema_registry_error *error) {
    if (error == NULL) {
        log_warn("Error pointer passed is NULL", ERR_CODE_NULL_PARAMETERS);
        return;
    }

    if (error->msg != NULL) {
        free(error->msg);
        error->msg = NULL;
    }
    error->code = 0;

    free(error);
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

//Create and set the error to the glue_schema_registry_error pointer holder.
void throw_error(glue_schema_registry_error **p_err, const char *msg, int code) {
    if (p_err == NULL) {
        return;
    }

    glue_schema_registry_error *err = new_glue_schema_registry_error(msg, code);
    *p_err = err;
}
