%module GsrSerDe
%{
#include "glue_schema_registry_deserializer.h"
%}
%include "glue_schema_registry_exception_interceptor.i"

typedef struct glue_schema_registry_deserializer {

    //Transfers the ownership of return pointer to target language.
    //so that the language can properly dispose after usage.
    %newobject decode;
    %newobject decode_schema;

    %extend {
        //Exception argument will be intercepted and thrown as exception in target language.
        //It is 2nd argument as there is no '$self' argument passed for constructor methods.
        %exception new_glue_schema_registry_deserializer %glue_schema_registry_exception_interceptor(arg2)
        glue_schema_registry_deserializer(const char *config_file_path, glue_schema_registry_error **p_err);

        ~glue_schema_registry_deserializer();

        //Note that the argument is '3' because the first argument is '$self'
        %exception decode %glue_schema_registry_exception_interceptor(arg3)
        mutable_byte_array *decode(read_only_byte_array *array, glue_schema_registry_error **p_err);

        //Note that the argument is '3' because the first argument is '$self'
        %exception can_decode %glue_schema_registry_exception_interceptor(arg3)
        bool can_decode(read_only_byte_array *array, glue_schema_registry_error **p_err);

        //Note that the argument is '3' because the first argument is '$self'
        %exception decode_schema %glue_schema_registry_exception_interceptor(arg3)
        glue_schema_registry_schema* decode_schema(read_only_byte_array *array, glue_schema_registry_error **p_err);
    }
} glue_schema_registry_deserializer;
