#include "../include/read_only_byte_array.h"
#include "../include/error_handling.h"
#include <stdio.h>


read_only_byte_array * new_read_only_byte_array(unsigned char *data, size_t len) {
    read_only_byte_array *array = NULL;
    if (data == NULL || len == 0) {
        log_error("Data is NULL or is of zero-length", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }
    array = (read_only_byte_array *) malloc(sizeof(read_only_byte_array));

    array->data = data;
    array->len = len;

    return array;
}


void delete_read_only_byte_array(read_only_byte_array *array) {
    if (array == NULL) {
        return;
    }
    array->len = 0;
    free(array);
}

unsigned char * read_only_byte_array_get_data(read_only_byte_array *array) {
    return array->data;
}

size_t read_only_byte_array_get_len(read_only_byte_array *array) {
    return array->len;
}