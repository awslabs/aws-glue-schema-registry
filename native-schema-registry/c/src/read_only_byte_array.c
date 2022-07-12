#include "../include/read_only_byte_array.h"
#include "../include/memory_allocator.h"
#include <stdio.h>


read_only_byte_array * new_read_only_byte_array(unsigned char *data, size_t len, glue_schema_registry_error **p_err) {
    read_only_byte_array *array = NULL;
    if (data == NULL || len == 0) {
        throw_error(p_err, "Data is NULL or is of zero-length", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }
    array = (read_only_byte_array *) aws_common_malloc(sizeof(read_only_byte_array));

    array->data = data;
    array->len = len;

    if (p_err != NULL) {
        *p_err = NULL;
    }
    return array;
}


void delete_read_only_byte_array(read_only_byte_array *array) {
    if (array == NULL) {
        return;
    }
    array->len = 0;
    aws_common_free(array);
}

unsigned char * read_only_byte_array_get_data(read_only_byte_array *array) {
    return array->data;
}

size_t read_only_byte_array_get_len(read_only_byte_array *array) {
    return array->len;
}
