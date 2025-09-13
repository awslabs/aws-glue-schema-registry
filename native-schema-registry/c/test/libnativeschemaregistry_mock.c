#include <string.h>
#include <stdlib.h>
#include "cmocka.h"
#include "glue_schema_registry_serializer.h"
#include "glue_schema_registry_test_helper.h"
#include "libnativeschemaregistry.h"
#include "libnativeschemaregistry_mock.h"
#include "memory_allocator.h"

//Mocks the functions provided by GraalVM image.

static int MOCK_STATE = UNINITIALIZED;
static graal_isolate_t* current_isolate = NULL;
static graal_isolatethread_t* main_thread = NULL;

void set_mock_state(int state) {
    MOCK_STATE = state;
}

void clear_mock_state(void) {
    MOCK_STATE = UNINITIALIZED;
    
    // Clean up any remaining allocated resources to prevent memory leaks
    if (main_thread != NULL) {
        aws_common_free(main_thread);
        main_thread = NULL;
    }
    
    if (current_isolate != NULL) {
        aws_common_free(current_isolate);
        current_isolate = NULL;
    }
}

static void validate_mock_state() {
    if (MOCK_STATE == UNINITIALIZED) {
        fprintf(stderr, "TEST ERROR: Mock not initialized. Invalid state\n");
        assert_true(0);
    }
}

int graal_create_isolate(graal_create_isolate_params_t* params, graal_isolate_t** isolate, graal_isolatethread_t** thread) {
    validate_mock_state();

    assert_null(params);
    assert_non_null(isolate);
    assert_non_null(thread);

    if (MOCK_STATE == GRAAL_VM_INIT_FAIL) {
        return -1;
    }
    current_isolate = (graal_isolate_t*)aws_common_malloc(8);
    main_thread = (graal_isolatethread_t*)aws_common_malloc(8);
    *isolate = current_isolate;
    *thread = main_thread;
    return 0;
}

int graal_tear_down_isolate(graal_isolatethread_t* isolateThread) {
    validate_mock_state();

    // Clean up tracked resources
    if (main_thread != NULL) {
        aws_common_free(main_thread);
        main_thread = NULL;
    }
    
    if (current_isolate != NULL) {
        aws_common_free(current_isolate);
        current_isolate = NULL;
    }
    
    // Free the passed thread only if it's different from main_thread to avoid double-free
    if (isolateThread != NULL && isolateThread != main_thread) {
        aws_common_free(isolateThread);
    }
    
    if (MOCK_STATE == TEAR_DOWN_FAIL) {
        return -1;
    }
    return 0;
}

int graal_attach_thread(graal_isolate_t* isolate, graal_isolatethread_t** thread) {
    validate_mock_state();

    if (MOCK_STATE == ATTACH_THREAD_FAIL) {
        return -1;
    }

    *thread = (graal_isolatethread_t*)aws_common_malloc(8);
    return 0;
}

int graal_detach_thread(graal_isolatethread_t* thread) {
    validate_mock_state();

    if (thread != NULL) {
        aws_common_free(thread);
    }

    if (MOCK_STATE == DETACH_THREAD_FAIL) {
        return -1;
    }

    return 0;
}

void initialize_serializer(graal_isolatethread_t* thread) {
    validate_mock_state();

    assert_non_null(thread);
    //do nothing
}

void initialize_deserializer(graal_isolatethread_t* thread) {
    validate_mock_state();

    assert_non_null(thread);
    //do nothing
}

int initialize_serializer_with_config(graal_isolatethread_t* thread, char* config_file_path, glue_schema_registry_error** p_err) {
    validate_mock_state();

    assert_non_null(thread);
    // config_file_path can be NULL for default configuration

    if (MOCK_STATE == CONFIG_INIT_SERIALIZER_FAIL) {
        *p_err = new_glue_schema_registry_error("Failed to initialize serializer with configuration file.", ERR_CODE_RUNTIME_ERROR);
        return -1;
    }

    return 0;
}

int initialize_deserializer_with_config(graal_isolatethread_t* thread, char* config_file_path, glue_schema_registry_error** p_err) {
    validate_mock_state();

    assert_non_null(thread);
    // config_file_path can be NULL for default configuration

    if (MOCK_STATE == CONFIG_INIT_DESERIALIZER_FAIL) {
        *p_err = new_glue_schema_registry_error("Configuration initialization failed for deserializer", ERR_CODE_RUNTIME_ERROR);
        return -1;
    }

    return 0;
}

mutable_byte_array* encode_with_schema(
        graal_isolatethread_t* isolatethread,
        read_only_byte_array* byte_array,
        char* transport_name,
        glue_schema_registry_schema* schema,
        glue_schema_registry_error** p_err) {
    validate_mock_state();

    assert_non_null(isolatethread);
    assert_non_null(byte_array);
    assert_non_null(transport_name);
    assert_non_null(schema);

    if (MOCK_STATE == ENCODE_FAILURE) {
        *p_err = new_glue_schema_registry_error("Encoding failed", ERR_CODE_RUNTIME_ERROR);
        return NULL;
    }

    return get_mut_byte_array_fixture();
}

mutable_byte_array* decode(
        graal_isolatethread_t* isolatethread,
        read_only_byte_array* byte_array,
        glue_schema_registry_error** p_err) {
    validate_mock_state();

    assert_non_null(isolatethread);
    assert_non_null(byte_array);

    if (MOCK_STATE == DECODE_FAILURE) {
        *p_err = new_glue_schema_registry_error("Decoding failed", ERR_CODE_RUNTIME_ERROR);
        return NULL;
    }

    return get_mut_byte_array_fixture();
}

glue_schema_registry_schema *decode_schema(
        graal_isolatethread_t* isolatethread,
        read_only_byte_array* byte_array,
        glue_schema_registry_error** p_err) {
    validate_mock_state();

    assert_non_null(isolatethread);
    assert_non_null(byte_array);

    if (MOCK_STATE == DECODE_SCHEMA_FAILURE) {
        *p_err = new_glue_schema_registry_error("Decoding schema failed", ERR_CODE_RUNTIME_ERROR);
        return NULL;
    }

    return get_gsr_schema_fixture();
}

char can_decode(
        graal_isolatethread_t* isolatethread,
        read_only_byte_array* byte_array,
        glue_schema_registry_error** p_err) {
    validate_mock_state();

    assert_non_null(isolatethread);
    assert_non_null(byte_array);

    if (MOCK_STATE == CAN_DECODE_FAILURE) {
        *p_err = new_glue_schema_registry_error("Can decode failed", ERR_CODE_RUNTIME_ERROR);
        return 0;
    }

    return 1;
}
