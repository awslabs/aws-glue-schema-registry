#ifndef MUTABLE_BYTE_ARRAY_H
#define MUTABLE_BYTE_ARRAY_H
#include <stdlib.h>
/**
* A mutable byte array that allows write / updating bytes in a fixed array of size `max_len`.
*/
typedef struct mutable_byte_array {
    unsigned char * data;
    size_t max_len;
} mutable_byte_array;

/**
 * Initializes a mutable byte array of size `len`
 * The data is initially set to '0'
 */
mutable_byte_array * new_mutable_byte_array(size_t len);

/**
 * Free the data and the pointer to the mutable byte array.
 */
void delete_mutable_byte_array(mutable_byte_array * array);

/**
 * Get the reference to the array contents.
 */
unsigned char * mutable_byte_array_get_data(mutable_byte_array * array);

/**
 * Writes a single byte at given index in the byte array.
 */
void mutable_byte_array_write(mutable_byte_array * array, size_t index, unsigned char byte);

/**
 * Return the len of the byte-array
 */
size_t mutable_byte_array_get_max_len(mutable_byte_array * array);

#endif //MUTABLE_BYTE_ARRAY_H