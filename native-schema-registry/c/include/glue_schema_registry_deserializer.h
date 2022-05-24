#ifndef GLUE_SCHEMA_REGISTRY_DESERIALIZER_H
#define GLUE_SCHEMA_REGISTRY_DESERIALIZER_H

#include "glue_schema_registry_schema.h"
#include "mutable_byte_array.h"
#include "read_only_byte_array.h"
#include <stdbool.h>

typedef struct glue_schema_registry_deserializer {
    //This is used for storing the instance context. Currently, being used for managing GraalVM instance.
    void *instance_context;
} glue_schema_registry_deserializer;

glue_schema_registry_deserializer *new_glue_schema_registry_deserializer(void);

void delete_glue_schema_registry_deserializer(glue_schema_registry_deserializer *deserializer);

mutable_byte_array *glue_schema_registry_deserializer_decode(glue_schema_registry_deserializer *deserializer,
                                                             read_only_byte_array *array);

glue_schema_registry_schema *glue_schema_registry_deserializer_decode_schema(glue_schema_registry_deserializer *deserializer,
                                                             read_only_byte_array *array);

bool glue_schema_registry_deserializer_can_decode(glue_schema_registry_deserializer *deserializer,
                                                                             read_only_byte_array *array);

#endif //GLUE_SCHEMA_REGISTRY_DESERIALIZER_H
