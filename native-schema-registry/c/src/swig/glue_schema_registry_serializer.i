%module GsrSerDe
%{
#include "glue_schema_registry_serializer.h"

%}

%include "glue_schema_registry_exception_interceptor.i"

typedef struct glue_schema_registry_serializer {
    #if defined(SWIGCSHARP)
        //Pass the ownership to C# runtime.
        %newobject encode;
    #endif
    %extend {
        //Exception argument will be intercepted and thrown as exception in target language.
        //It is 1st argument as there is no '$self' argument passed for constructor methods.
        %exception new_glue_schema_registry_serializer %glue_schema_registry_exception_interceptor(arg1)
        glue_schema_registry_serializer(glue_schema_registry_error **p_err);

        ~glue_schema_registry_serializer();

        //Note that the argument is '5' because the first argument is '$self'
        %exception encode %glue_schema_registry_exception_interceptor(arg5)
        mutable_byte_array *encode(read_only_byte_array *array,
                                   const char *transport_name,
                                   glue_schema_registry_schema *gsr_schema,
                                   glue_schema_registry_error **p_err);
    }
} glue_schema_registry_serializer;
