%module GsrSerDe
%{
#include "glue_schema_registry_schema.h"
%}

//Make Swig aware of this structure to generate classes in target languages.
typedef struct glue_schema_registry_schema {
    //We extend the glue_schema_registry_schema structure to generate
    //constructors and destructors in target language.
    //Swig automatically maps to corresponding methods in C implementation.
    %extend {

        //Intercept the p_error and throw as target language exception.
        %exception new_glue_schema_registry_schema %glue_schema_registry_exception_interceptor(arg4);
        //Maps to new_glue_schema_registry_schema
        glue_schema_registry_schema(
                const char * schema_name,
                const char * schema_def,
                const char * data_format,
                glue_schema_registry_error **p_err
        );

        //Maps to delete_glue_schema_registry_schema
        ~glue_schema_registry_schema();

        //Maps to glue_schema_registry_get_* methods
        const char * get_schema_name();
        const char * get_schema_def();
        const char * get_data_format();
    }
} glue_schema_registry_schema;
