%module GsrSerDe
%{
#include "read_only_byte_array.h"
%}

#if defined(SWIGPYTHON)
%include "pybuffer.i"
#endif

typedef struct read_only_byte_array {
    %extend {
//PyBuffer provides type-maps for accepting Python bytes.
#if defined(SWIGPYTHON)
        %pybuffer_binary(unsigned char * data, size_t len);
#endif
        read_only_byte_array(unsigned char *data, size_t len);

        ~read_only_byte_array();

        unsigned char * get_data();

        size_t get_len();
    }
} read_only_byte_array;
