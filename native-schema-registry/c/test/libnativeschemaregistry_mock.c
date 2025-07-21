#include <string.h>
#include <stdlib.h>
#include "cmocka.h"
#include "glue_schema_registry_serializer.h"
#include "glue_schema_registry_test_helper.h"
#include "libnativeschemaregistry.h"
#include "libnativeschemaregistry_mock.h"

//Mocks the functions provided by GraalVM image.

static int MOCK_STATE = UNINITIALIZED;

void set_mock_state(int state) {
    MOCK_STATE = state;
}

void clear_mock_state(void) {
    MOCK_STATE = UNINITIALIZED;
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
    assert_null(isolate);
    assert_non_null(thread);

    *thread = (graal_isolatethread_t*) malloc(sizeof(graal_isolatethread_t*));
    if (MOCK_STATE == GRAAL_VM_INIT_FAIL) {
        return -1;
    }
    return 0;
}

int graal_tear_down_isolate(graal_isolatethread_t* isolateThread) {
    validate_mock_state();

    free(isolateThread);
    if (MOCK_STATE == TEAR_DOWN_FAIL) {
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
