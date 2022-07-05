#include "../include/glue_schema_registry_schema.h"
#include "../include/glue_schema_registry_error.h"
#include <stdlib.h>
#include <string.h>

static int validate(const char *schema_name, const char *schema_def, const char *data_format) {
    if (schema_name == NULL || schema_def == NULL || data_format == NULL) {
        return 1;
    }

    return 0;
}

glue_schema_registry_schema * new_glue_schema_registry_schema(
        const char * schema_name,
        const char * schema_def,
        const char * data_format,
        glue_schema_registry_error **p_err) {
    if (validate(schema_name, schema_def, data_format) != 0) {
        throw_error(p_err, "Schema parameters are NULL", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }
    glue_schema_registry_schema * glueSchemaRegistrySchema = NULL;
    glueSchemaRegistrySchema = (glue_schema_registry_schema *) malloc(sizeof(glue_schema_registry_schema));

    glueSchemaRegistrySchema->schema_name = strdup(schema_name);
    glueSchemaRegistrySchema->schema_def = strdup(schema_def);
    glueSchemaRegistrySchema->data_format = strdup(data_format);

    if (p_err != NULL) {
        *p_err = NULL;
    }

    return glueSchemaRegistrySchema;
}

void delete_glue_schema_registry_schema(glue_schema_registry_schema * glueSchemaRegistrySchema) {
    if (glueSchemaRegistrySchema == NULL) {
        log_warn("Schema instance is NULL", ERR_CODE_NULL_PARAMETERS);
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
