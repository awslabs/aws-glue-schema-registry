#ifndef READ_ONLY_BYTE_ARRAY_H
#define READ_ONLY_BYTE_ARRAY_H
#include <stdlib.h>
#include "../include/glue_schema_registry_error.h"

typedef struct read_only_byte_array {
    unsigned char * data;
    size_t len;
} read_only_byte_array;

/**
 * Creates a read-only byte array that points to given memory location to
 * provide a view over the data. Attempts to modify the data can result in
 * unintended consequences or crashes.
 * The caller must guarantee the memory is valid and is of exactly `len`
 * Caller can optionally provide pointer holder to glue_schema_registry_error to read error messages.
 */
read_only_byte_array * new_read_only_byte_array(unsigned char *data, size_t len, glue_schema_registry_error **p_err);

/**
 * Deletes the byte array instance but not the underlying data.
 */
void delete_read_only_byte_array(read_only_byte_array * array);

/**
 * Gets the reference to data pointed by this array.
 */
unsigned char * read_only_byte_array_get_data(read_only_byte_array * array);

/**
 * Gets the len of the data being pointed by this array.
 */
size_t read_only_byte_array_get_len(read_only_byte_array * array);

#endif //READ_ONLY_BYTE_ARRAY_H