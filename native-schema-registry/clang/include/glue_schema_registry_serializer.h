#ifndef GLUE_SCHEMA_REGISTRY_SERIALIZER_H
#define GLUE_SCHEMA_REGISTRY_SERIALIZER_H

#include "glue_schema_registry_schema.h"

typedef struct glue_schema_registry_serializer {
    //This is used for storing the instance context. Currently being used for managing GraalVM instance.
    void *instance_context;
} glue_schema_registry_serializer;

glue_schema_registry_serializer *new_glue_schema_registry_serializer();

void delete_glue_schema_registry_serializer(glue_schema_registry_serializer *serializer);

//Encodes the GSR Schema with a byte array.
//The byte array will be added.
glue_schema_registry_schema *glue_schema_registry_serializer_encode(glue_schema_registry_serializer *serializer,
                                                                    glue_schema_registry_schema *gsr_schema);

#endif //GLUE_SCHEMA_REGISTRY_SERIALIZER_H
