#ifndef GLUE_SCHEMA_REGISTRY_SERIALIZER_H
#define GLUE_SCHEMA_REGISTRY_SERIALIZER_H

#include "glue_schema_registry_schema.h"
#include "glue_schema_registry_error.h"
#include "mutable_byte_array.h"
#include "read_only_byte_array.h"

typedef struct glue_schema_registry_serializer {
    //This is used for storing the instance context. Currently being used for managing GraalVM instance.
    void *instance_context;
} glue_schema_registry_serializer;

glue_schema_registry_serializer *new_glue_schema_registry_serializer(glue_schema_registry_error **p_err);

glue_schema_registry_serializer *new_glue_schema_registry_serializer_with_config(const char *config_file_path, glue_schema_registry_error **p_err);

void delete_glue_schema_registry_serializer(glue_schema_registry_serializer *serializer);

//Encodes the GSR Schema with a byte array.
mutable_byte_array *glue_schema_registry_serializer_encode(glue_schema_registry_serializer *serializer,
                                                                    read_only_byte_array * array,
                                                                    const char * transport_name,
                                                                    glue_schema_registry_schema *gsr_schema,
                                                                    glue_schema_registry_error **p_err);

#endif //GLUE_SCHEMA_REGISTRY_SERIALIZER_H
