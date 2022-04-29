#include "../include/glue_schema_registry_serializer.h"
#include "../../target/libnativeschemaregistry.h"
#include <stdio.h>
#include <stdlib.h>

glue_schema_registry_serializer *new_glue_schema_registry_serializer() {
    glue_schema_registry_serializer *serializer = NULL;
    serializer = (glue_schema_registry_serializer *) malloc(sizeof(glue_schema_registry_serializer));

    //Initializes a GraalVM instance to call the entry points.
    int ret = graal_create_isolate(NULL, NULL, (graal_isolatethread_t **) &serializer->instance_context);

    if (ret != 0) {
        fprintf(stderr, "Failed to initialize GraalVM isolate: %d\n", ret);
        return NULL;
    }
    return serializer;
}

void delete_glue_schema_registry_serializer(glue_schema_registry_serializer *serializer) {
    //Tear down the GraalVM instance.
    if (serializer == NULL) {
        return;
    }
    if (serializer->instance_context != NULL) {
        graal_tear_down_isolate(serializer->instance_context);
        serializer->instance_context = NULL;
    }

    free(serializer);
}

glue_schema_registry_schema *glue_schema_registry_serializer_encode(glue_schema_registry_serializer *serializer,
                                                                    glue_schema_registry_schema *gsr_schema) {
    if (serializer == NULL) {
        fprintf(stderr, "Serializer instance is null\n");
        return NULL;
    }

    //TODO: Pass a byte_buffer
    return encode_with_schema(serializer->instance_context, gsr_schema);
}