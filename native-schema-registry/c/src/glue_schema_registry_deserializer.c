#include "../include/glue_schema_registry_deserializer.h"
#include "../include/error_handling.h"
#include "../../target/libnativeschemaregistry.h"
#include <stdlib.h>

glue_schema_registry_deserializer * new_glue_schema_registry_deserializer() {
    glue_schema_registry_deserializer *deserializer = NULL;
    deserializer =
            (glue_schema_registry_deserializer *) malloc(sizeof(glue_schema_registry_deserializer));

    int ret = graal_create_isolate(NULL, NULL, (graal_isolatethread_t **) &deserializer->instance_context);
    if (ret != 0) {
        log_error("Failed to initialize GraalVM isolate.", ERR_CODE_GRAALVM_INIT_EXCEPTION);
        delete_glue_schema_registry_deserializer(deserializer);
        return NULL;
    }
    //TODO: Handle errors here.
    initialize_deserializer(deserializer->instance_context);
    return deserializer;
}

void delete_glue_schema_registry_deserializer(glue_schema_registry_deserializer * deserializer) {
    if (deserializer == NULL) {
        log_error("Deserializer is NULL", ERR_CODE_NULL_PARAMETERS);
        return;
    }
    if (deserializer->instance_context != NULL) {
        int ret = graal_tear_down_isolate(deserializer->instance_context);
        if (ret != 0) {
            log_error("Error tearing down the graal isolate instance.", ERR_CODE_GRAALVM_TEARDOWN_EXCEPTION);
        }
        deserializer->instance_context = NULL;
    }

    free(deserializer);
}

mutable_byte_array *glue_schema_registry_deserializer_decode(glue_schema_registry_deserializer * deserializer, read_only_byte_array *array) {
    if (deserializer == NULL || deserializer->instance_context == NULL) {
        log_error("Deserializer instance or instance context is null.", ERR_CODE_INVALID_STATE);
        return NULL;
    }

    if (array == NULL || array->len == 0) {
        log_error("Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }

    return decode(deserializer->instance_context, array);
}

glue_schema_registry_schema *glue_schema_registry_deserializer_decode_schema(glue_schema_registry_deserializer * deserializer, read_only_byte_array *array) {
    if (deserializer == NULL || deserializer->instance_context == NULL) {
        log_error("Deserializer instance or instance context is null.", ERR_CODE_INVALID_STATE);
        return NULL;
    }

    if (array == NULL || array->len == 0) {
        log_error("Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }

    glue_schema_registry_schema * schema = decode_schema(deserializer->instance_context, array);
    return schema;
}

bool glue_schema_registry_deserializer_can_decode(glue_schema_registry_deserializer * deserializer, read_only_byte_array *array) {
    if (deserializer == NULL || deserializer->instance_context == NULL) {
        log_error("Deserializer instance or instance context is null.", ERR_CODE_INVALID_STATE);
        return NULL;
    }

    if (array == NULL || array->len == 0) {
        log_error("Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }

    return can_decode(deserializer->instance_context, array);
}

