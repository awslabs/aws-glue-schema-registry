#include "glue_schema_registry_deserializer.h"
#include "memory_allocator.h"
#include "libnativeschemaregistry.h"
#include <stdlib.h>

typedef struct {
    graal_isolate_t *isolate;
} deserializer_context;

glue_schema_registry_deserializer * new_glue_schema_registry_deserializer(const char *config_file_path, glue_schema_registry_error **p_err) {
    glue_schema_registry_deserializer *deserializer = NULL;
    deserializer =
            (glue_schema_registry_deserializer *) aws_common_malloc(sizeof(glue_schema_registry_deserializer));
    deserializer->instance_context = NULL;  // Initialize to prevent segfault on cleanup

    deserializer_context *ctx = aws_common_malloc(sizeof(deserializer_context));
    ctx->isolate = NULL;
    graal_isolatethread_t *thread = NULL;

    int ret = graal_create_isolate(NULL, &ctx->isolate, &thread);
    if (ret != 0) {
        aws_common_free(ctx);
        delete_glue_schema_registry_deserializer(deserializer);
        throw_error(p_err, "Failed to initialize GraalVM isolate.", ERR_CODE_GRAALVM_INIT_EXCEPTION);
        return NULL;
    }

    deserializer->instance_context = ctx;

    //Initialize with configuration file (can be NULL for default configuration)
    int config_result = initialize_deserializer_with_config(thread, (char*)config_file_path, p_err);
    
    if (config_result != 0) {
        delete_glue_schema_registry_deserializer(deserializer);
        // Only throw an error if one wasn't already set by initialize_deserializer_with_config
        if (p_err != NULL) {
            throw_error(p_err, "Failed to initialize deserializer with configuration file.", ERR_CODE_RUNTIME_ERROR);
        }
        return NULL;
    }

    return deserializer;
}

void delete_glue_schema_registry_deserializer(glue_schema_registry_deserializer * deserializer) {
    if (deserializer == NULL) {
        log_warn("Deserializer is NULL", ERR_CODE_NULL_PARAMETERS);
        return;
    }
    if (deserializer->instance_context != NULL) {
        deserializer_context *ctx = (deserializer_context*)deserializer->instance_context;

        if (ctx->isolate != NULL) {
            graal_isolatethread_t *thread = NULL;
            if (graal_attach_thread(ctx->isolate, &thread) == 0) {
                if (graal_tear_down_isolate(thread) != 0) {
                    log_warn("Failed to tear down GraalVM isolate in deserializer", ERR_CODE_GRAALVM_TEARDOWN_EXCEPTION);
                }
            }
        }

        aws_common_free(ctx);
        deserializer->instance_context = NULL;
    }

    aws_common_free(deserializer);
}

static bool validate(
        glue_schema_registry_deserializer *deserializer,
        read_only_byte_array *array,
        glue_schema_registry_error **p_err) {
    if (deserializer == NULL || deserializer->instance_context == NULL) {
        throw_error(p_err, "Deserializer instance or instance context is null.", ERR_CODE_INVALID_STATE);
        return false;
    }

    if (array == NULL || array->len == 0) {
        throw_error(p_err, "Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);
        return false;
    }
    return true;
}

mutable_byte_array *glue_schema_registry_deserializer_decode(glue_schema_registry_deserializer * deserializer,
                                                             read_only_byte_array *array,
                                                             glue_schema_registry_error **p_err) {
    if (!validate(deserializer, array, p_err)) {
        return NULL;
    }

    deserializer_context *ctx = (deserializer_context*)deserializer->instance_context;
    graal_isolatethread_t *thread = NULL;

    int attach_result = graal_attach_thread(ctx->isolate, &thread);
    if (attach_result != 0) {
        throw_error(p_err, "Failed to attach thread to GraalVM isolate", ERR_CODE_GRAAL_ATTACH_FAILED);
        return NULL;
    }

    mutable_byte_array *result = decode(thread, array, p_err);

    if (graal_detach_thread(thread) != 0) {
        log_warn("Failed to detach thread from GraalVM isolate in deserializer decode", ERR_CODE_GRAAL_DETACH_FAILED);
    }
    return result;
}

glue_schema_registry_schema *glue_schema_registry_deserializer_decode_schema(glue_schema_registry_deserializer * deserializer,
                                                                             read_only_byte_array *array,
                                                                             glue_schema_registry_error **p_err) {
    if (!validate(deserializer, array, p_err)) {
        return NULL;
    }

    deserializer_context *ctx = (deserializer_context*)deserializer->instance_context;
    graal_isolatethread_t *thread = NULL;

    int attach_result = graal_attach_thread(ctx->isolate, &thread);
    if (attach_result != 0) {
        throw_error(p_err, "Failed to attach thread to GraalVM isolate", ERR_CODE_GRAAL_ATTACH_FAILED);
        return NULL;
    }

    glue_schema_registry_schema *schema = decode_schema(thread, array, p_err);

    if (graal_detach_thread(thread) != 0) {
        log_warn("Failed to detach thread from GraalVM isolate in deserializer decode_schema", ERR_CODE_GRAAL_DETACH_FAILED);
    }
    return schema;
}

bool glue_schema_registry_deserializer_can_decode(glue_schema_registry_deserializer * deserializer,
                                                  read_only_byte_array *array,
                                                  glue_schema_registry_error **p_err) {
    if (!validate(deserializer, array, p_err)) {
        return false;
    }

    deserializer_context *ctx = (deserializer_context*)deserializer->instance_context;
    graal_isolatethread_t *thread = NULL;

    int attach_result = graal_attach_thread(ctx->isolate, &thread);
    if (attach_result != 0) {
        throw_error(p_err, "Failed to attach thread to GraalVM isolate", ERR_CODE_GRAAL_ATTACH_FAILED);
        return false;
    }

    bool result = can_decode(thread, array, p_err);

    if (graal_detach_thread(thread) != 0) {
        log_warn("Failed to detach thread from GraalVM isolate in deserializer can_decode", ERR_CODE_GRAAL_DETACH_FAILED);
    }
    return result;
}
