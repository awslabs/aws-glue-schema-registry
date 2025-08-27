#include "mutable_byte_array.h"
#include "glue_schema_registry_error.h"
#include "memory_allocator.h"
#include <stdio.h>

/**
 * Creates a mutable byte array that manages an array and provides API to update it.
 */
mutable_byte_array *new_mutable_byte_array(size_t max_len, glue_schema_registry_error **p_err) {
    if (max_len == 0) {
        throw_error(p_err, "Cannot create byte array of size 0", ERR_CODE_INVALID_PARAMETERS);
        return NULL;
    }
    if (max_len >= MAX_BYTES_LIMIT) {
        throw_error(p_err, "max_len cannot be greater than limit: 2147483647", ERR_CODE_INVALID_PARAMETERS);
        return NULL;
    }
    mutable_byte_array *array = NULL;
    array = (mutable_byte_array *) aws_common_malloc(sizeof(mutable_byte_array));
    array->data = (unsigned char*) aws_common_calloc(max_len, sizeof(unsigned char));
    array->max_len = max_len;

    //Explicitly set to null
    if (p_err != NULL) {
        *p_err = NULL;
    }

    return array;
}

/**
 * Deletes the byte array.
 */
void delete_mutable_byte_array(mutable_byte_array *array) {
    if (array == NULL) {
        return;
    }

    if (array->data != NULL) {
        for (size_t index = 0; index < array->max_len; index++) {
            array->data[index] = 0;
        }
        aws_common_free(array->data);
        array->data = NULL;
    }
    array->max_len = 0;
    aws_common_free(array);
}

void mutable_byte_array_write(mutable_byte_array *array, size_t index, unsigned char byte,
                              glue_schema_registry_error **p_err) {
    if (array == NULL) {
        throw_error(p_err, "Cannot write to NULL byte array", ERR_CODE_NULL_PARAMETERS);
        return;
    }
    if (index >= array->max_len) {
        char error_msg[MAX_ERROR_MSG_LEN];
        sprintf(error_msg, "Index: %ld out of range for the byte-array of max_len: %ld", index, array->max_len);
        throw_error(p_err, error_msg, ERR_CODE_INVALID_PARAMETERS);
        return;
    }

    array->data[index] = byte;
    //Explicitly set that there was no error.
    if (p_err != NULL) {
        *p_err = NULL;
    }
}

unsigned char * mutable_byte_array_get_data(mutable_byte_array *array) {
    return array->data;
}

size_t mutable_byte_array_get_max_len(mutable_byte_array *array) {
    return array->max_len;
}
