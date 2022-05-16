#include "../include/glue_schema_registry_serializer.h"
#include "../include/error_handling.h"
#include "../../target/libnativeschemaregistry.h"
#include <stdlib.h>

glue_schema_registry_serializer *new_glue_schema_registry_serializer() {
    glue_schema_registry_serializer *serializer = NULL;
    serializer = (glue_schema_registry_serializer *) malloc(sizeof(glue_schema_registry_serializer));

    //Initializes a GraalVM instance to call the entry points.
    int ret = graal_create_isolate(NULL, NULL, (graal_isolatethread_t **) &serializer->instance_context);

    if (ret != 0) {
        log_error("Failed to initialize GraalVM isolate.", ERR_CODE_GRAALVM_INIT_EXCEPTION);
        delete_glue_schema_registry_serializer(serializer);
        return NULL;
    }
    //TODO: Handle errors here.
    initialize_serializer(serializer->instance_context);
    return serializer;
}

void delete_glue_schema_registry_serializer(glue_schema_registry_serializer *serializer) {
    //Tear down the GraalVM instance.
    if (serializer == NULL) {
        log_error("Serializer is NULL", ERR_CODE_NULL_PARAMETERS);
        return;
    }
    if (serializer->instance_context != NULL) {
        int ret = graal_tear_down_isolate(serializer->instance_context);
        if (ret != 0) {
            log_error("Error tearing down the graal isolate instance.", ERR_CODE_GRAALVM_TEARDOWN_EXCEPTION);
        }
        serializer->instance_context = NULL;
    }

    free(serializer);
}

mutable_byte_array *glue_schema_registry_serializer_encode(
        glue_schema_registry_serializer *serializer,
        read_only_byte_array *array,
        const char * transport_name,
        glue_schema_registry_schema *gsr_schema) {
    if (serializer == NULL || serializer->instance_context == NULL) {
        log_error("Serializer instance or instance context is null.", ERR_CODE_INVALID_STATE);
        return NULL;
    }

    if (gsr_schema == NULL) {
        log_error("Schema passed cannot be null", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }

    if (array == NULL || array->len == 0) {
        log_error("Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }

    return encode_with_schema(serializer->instance_context, array, transport_name, gsr_schema);
}
