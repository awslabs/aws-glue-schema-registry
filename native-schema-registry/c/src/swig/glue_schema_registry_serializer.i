%module GsrSerDe
%{
#include "glue_schema_registry_serializer.h"

%}
typedef struct glue_schema_registry_serializer {
    %extend {
        glue_schema_registry_serializer();

        ~glue_schema_registry_serializer();

        mutable_byte_array *encode(read_only_byte_array *array,
                                   const char *transport_name,
                                   glue_schema_registry_schema *gsr_schema);
    }
} glue_schema_registry_serializer;
