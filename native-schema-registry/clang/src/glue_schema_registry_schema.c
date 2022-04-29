#include "../include/glue_schema_registry_schema.h"
#include <stdlib.h>
#include <string.h>

int validate(const char *schema_name, const char *schema_def, const char *data_format) {
    if (schema_name == NULL || schema_def == NULL || data_format == NULL) {
        return 1;
    }

    return 0;
}

glue_schema_registry_schema * new_glue_schema_registry_schema(
        const char * schema_name,
        const char * schema_def,
        const char * data_format) {
    if (validate(schema_name, schema_def, data_format) != 0) {
        return NULL;
    }
    glue_schema_registry_schema * glueSchemaRegistrySchema = NULL;
    glueSchemaRegistrySchema = (glue_schema_registry_schema *) malloc(sizeof(glue_schema_registry_schema));

    glueSchemaRegistrySchema->schema_name = strdup(schema_name);
    glueSchemaRegistrySchema->schema_def = strdup(schema_def);
    glueSchemaRegistrySchema->data_format = strdup(data_format);

    return glueSchemaRegistrySchema;
}

void delete_glue_schema_registry_schema(glue_schema_registry_schema * glueSchemaRegistrySchema) {
    if (glueSchemaRegistrySchema == NULL) {
        return;
    }

    if (glueSchemaRegistrySchema->schema_name != NULL) {
        free(glueSchemaRegistrySchema->schema_name);
    }
    if (glueSchemaRegistrySchema->schema_def != NULL) {
        free(glueSchemaRegistrySchema->schema_def);
    }
    if (glueSchemaRegistrySchema->data_format != NULL) {
        free(glueSchemaRegistrySchema->data_format);
    }
    free(glueSchemaRegistrySchema);
}

const char * glue_schema_registry_schema_get_schema_name(glue_schema_registry_schema * glueSchemaRegistrySchema) {
    return glueSchemaRegistrySchema->schema_name;
}

const char * glue_schema_registry_schema_get_schema_def(glue_schema_registry_schema * glueSchemaRegistrySchema) {
    return glueSchemaRegistrySchema->schema_def;
}

const char * glue_schema_registry_schema_get_data_format(glue_schema_registry_schema * glueSchemaRegistrySchema) {
    return glueSchemaRegistrySchema->data_format;
}