%module GsrSerDe
%{
#include "glue_schema_registry_deserializer.h"
%}

typedef struct glue_schema_registry_deserializer {

    %newobject decode_schema(read_only_byte_array *array);
    #if defined(SWIGCSHARP)
      %newobject decode(read_only_byte_array *array);
    #endif

    %extend {
        glue_schema_registry_deserializer();

        ~glue_schema_registry_deserializer();

        mutable_byte_array *decode(read_only_byte_array *array);

        bool can_decode(read_only_byte_array *array);

        glue_schema_registry_schema* decode_schema(read_only_byte_array *array);
    }
} glue_schema_registry_deserializer;
