%module GsrSerDe
%{
#include "read_only_byte_array.h"
%}

#if defined(SWIGPYTHON)
%include "pybuffer.i"
#endif

#if defined(SWIGCSHARP)
%include "arrays_csharp.i"
#endif

typedef struct read_only_byte_array {
    %extend {
//PyBuffer provides type-maps for accepting Python bytes.
#if defined(SWIGPYTHON)
        %pybuffer_binary(unsigned char * data, size_t len);
#endif
#if defined(SWIGCSHARP)
        //Typemap that converts the C# byte [] into unsigned char *
        %apply unsigned char INPUT[] {unsigned char *data};
#endif
        read_only_byte_array(unsigned char *data, size_t len);

        ~read_only_byte_array();

        unsigned char * get_data();

        size_t get_len();
    }
} read_only_byte_array;
