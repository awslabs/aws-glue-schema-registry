#include "../include/mutable_byte_array.h"
#include "../include/error_handling.h"
#include <stdio.h>

/**
 * Creates a mutable byte array that manages an array and provides API to update it.
 */
mutable_byte_array *new_mutable_byte_array(size_t max_len) {
    if (max_len == 0) {
        log_error("Cannot create byte array of size 0", ERR_CODE_INVALID_PARAMETERS);
        return NULL;
    }
    if (max_len >= MAX_BYTES_LIMIT) {
        log_error("max_len cannot be greater than limit: 2147483647", ERR_CODE_INVALID_PARAMETERS);
        return NULL;
    }
    mutable_byte_array *array = NULL;
    array = (mutable_byte_array *) malloc(sizeof(mutable_byte_array));
    array->data = (unsigned char*) calloc(max_len, sizeof(unsigned char));
    array->max_len = max_len;

    return array;
}

/**
 * Deletes the byte array.
 */
void delete_mutable_byte_array(mutable_byte_array *array) {
    if (array == NULL) {
        return;
    }

    //TODO: zero data?
    if (array->data != NULL) {
        free(array->data);
        array->data = NULL;
    }
    array->max_len = 0;
    free(array);
}

void mutable_byte_array_write(mutable_byte_array *array, size_t index, unsigned char byte) {
    if (array == NULL) {
        log_error("Cannot write to NULL byte array", ERR_CODE_NULL_PARAMETERS);
        return;
    }
    if (index >= array->max_len) {
        char error_msg[70];
        sprintf(error_msg, "Index: %ld out of range for the byte-array of max_len: %ld", index, array->max_len);
        log_error(error_msg, ERR_CODE_INVALID_PARAMETERS);
        return;
    }

    array->data[index] = byte;
}

unsigned char * mutable_byte_array_get_data(mutable_byte_array *array) {
    return array->data;
}

size_t mutable_byte_array_get_max_len(mutable_byte_array *array) {
    return array->max_len;
}
