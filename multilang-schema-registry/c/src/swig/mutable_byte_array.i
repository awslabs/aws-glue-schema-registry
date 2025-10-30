%module GsrSerDe
%{
#include "mutable_byte_array.h"
%}

#if defined(SWIGCSHARP)
%include "arrays_csharp.i"
#endif

#if defined(SWIGPYTHON)
%include "pybuffer.i"
#endif

%include "glue_schema_registry_exception_interceptor.i"

//Methods that map to the C implementation.
typedef struct mutable_byte_array {
    //Constructor methods are referred to as 'new_<object_name>'
    %extend {
        %exception new_mutable_byte_array %glue_schema_registry_exception_interceptor(arg2);
        mutable_byte_array(size_t len, glue_schema_registry_error **p_err);

        ~mutable_byte_array();
        //Uses array output typemap that copies the mutable_byte_array contents
        //into given array.
        //TODO: Optimization: We can avoid copying large data by figuring out a way
        //to expose underlying buffers.
        #if defined(SWIGCSHARP)
            %apply unsigned char OUTPUT[] {unsigned char *data};
            void get_data_copy(unsigned char *data) {
                memcpy(data, $self->data, $self->max_len);
            }
        #endif
        #if defined(SWIGPYTHON)
            %pybuffer_mutable_binary(unsigned char * data, size_t len);
            void get_data_copy(unsigned char *data, size_t len) {
                memcpy(data, $self->data, len);
            }
        #endif

        unsigned char * get_data();

        size_t get_max_len();
    }
} mutable_byte_array;
