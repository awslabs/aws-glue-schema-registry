#include "glue_schema_registry_serializer.h"
#include "memory_allocator.h"
#include "libnativeschemaregistry.h"
#include <stdlib.h>

typedef struct {
    graal_isolate_t *isolate;
} serializer_context;

glue_schema_registry_serializer *new_glue_schema_registry_serializer(const char *config_file_path, const char *user_agent, glue_schema_registry_error **p_err) {
    glue_schema_registry_serializer *serializer = NULL;
    serializer = (glue_schema_registry_serializer *) aws_common_malloc(sizeof(glue_schema_registry_serializer));
    serializer->instance_context = NULL;

    serializer_context *ctx = aws_common_malloc(sizeof(serializer_context));
    ctx->isolate = NULL;
    graal_isolatethread_t *thread = NULL;

    //Creates isolate that can be shared across threads
    int ret = graal_create_isolate(NULL, &ctx->isolate, &thread);

    if (ret != 0) {
        aws_common_free(ctx);
        delete_glue_schema_registry_serializer(serializer);
        throw_error(p_err, "Failed to initialize GraalVM isolate.", ERR_CODE_GRAALVM_INIT_EXCEPTION);
        return NULL;
    }

    serializer->instance_context = ctx;

    //Initialize with configuration file using main thread
    int config_result = initialize_serializer_with_config(thread, (char*)config_file_path, (char*)user_agent, p_err);
    
    if (config_result != 0) {
        delete_glue_schema_registry_serializer(serializer);
        if (p_err != NULL) {
            throw_error(p_err, "Failed to initialize serializer with configuration file.", ERR_CODE_RUNTIME_ERROR);
        }
        return NULL;
    }

    return serializer;
}

void delete_glue_schema_registry_serializer(glue_schema_registry_serializer *serializer) {
    if (serializer == NULL) {
        log_warn("Serializer is NULL", ERR_CODE_NULL_PARAMETERS);
        return;
    }
    if (serializer->instance_context != NULL) {
        serializer_context *ctx = (serializer_context*)serializer->instance_context;

        if (ctx->isolate != NULL) {
            graal_isolatethread_t *thread = NULL;
            if (graal_attach_thread(ctx->isolate, &thread) == 0) {
                if (graal_tear_down_isolate(thread) != 0) {
                    log_warn("Failed to tear down GraalVM isolate in serializer", ERR_CODE_GRAALVM_TEARDOWN_EXCEPTION);
                }
            }
        }

        aws_common_free(ctx);
        serializer->instance_context = NULL;
    }

    aws_common_free(serializer);
}

mutable_byte_array *glue_schema_registry_serializer_encode(
        glue_schema_registry_serializer *serializer,
        read_only_byte_array *array,
        const char * transport_name,
        glue_schema_registry_schema *gsr_schema,
        glue_schema_registry_error **p_err) {
    if (serializer == NULL || serializer->instance_context == NULL) {
        throw_error(p_err, "Serializer instance or instance context is null.", ERR_CODE_INVALID_STATE);
        return NULL;
    }

    if (gsr_schema == NULL) {
        throw_error(p_err, "Schema passed cannot be null", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }

    if (array == NULL || array->len == 0) {
        throw_error(p_err, "Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);
        return NULL;
    }

    serializer_context *ctx = (serializer_context*)serializer->instance_context;
    graal_isolatethread_t *thread = NULL;

    if (graal_attach_thread(ctx->isolate, &thread) != 0) {
        throw_error(p_err, "Failed to attach thread to GraalVM isolate", ERR_CODE_GRAAL_ATTACH_FAILED);
        return NULL;
    }

    mutable_byte_array *result = encode_with_schema(thread, array, (char*)transport_name, gsr_schema, p_err);

    if (graal_detach_thread(thread) != 0) {
        log_warn("Failed to detach thread from GraalVM isolate in serializer", ERR_CODE_GRAAL_DETACH_FAILED);
    }

    return result;
}
