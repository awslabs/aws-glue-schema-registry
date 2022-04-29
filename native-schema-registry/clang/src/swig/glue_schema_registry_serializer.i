%module GsrSerDe
%{
#include "glue_schema_registry_serializer.h"

%}
typedef struct glue_schema_registry_serializer {
    %extend {
        glue_schema_registry_serializer();

        ~glue_schema_registry_serializer();

        glue_schema_registry_schema *encode(glue_schema_registry_schema *gsr_schema);
    }
} glue_schema_registry_serializer;
