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

%include "glue_schema_registry_exception_interceptor.i"

typedef struct read_only_byte_array {
    %extend {
        %exception new_read_only_byte_array %glue_schema_registry_exception_interceptor(arg3);
        #if defined(SWIGCSHARP)
            //Typemap that converts the C# byte [] into unsigned char *
            %apply unsigned char INPUT[] {unsigned char *data};
        #endif
        //PyBuffer provides type-maps for accepting Python bytes.
        #if defined(SWIGPYTHON)
            %pybuffer_binary(unsigned char * data, size_t len);
        #endif
        read_only_byte_array(unsigned char *data, size_t len, glue_schema_registry_error **p_err);

        ~read_only_byte_array();

        unsigned char * get_data();

        size_t get_len();
    }
} read_only_byte_array;
