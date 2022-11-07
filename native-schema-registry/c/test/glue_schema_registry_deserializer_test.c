#include <string.h>
#include "cmocka.h"
#include "libnativeschemaregistry_mock.h"
#include "glue_schema_registry_test_helper.h"
#include "glue_schema_registry_deserializer.h"

static void test_new_glue_schema_registry_deserializer_created_successfully(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    glue_schema_registry_deserializer *gsr_deserializer = new_glue_schema_registry_deserializer(NULL);

    assert_non_null(gsr_deserializer);
    assert_non_null(gsr_deserializer->instance_context);
    assert_int_equal(sizeof(graal_isolatethread_t *), sizeof(gsr_deserializer->instance_context));
    delete_glue_schema_registry_deserializer(gsr_deserializer);

    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_init_fails_throws_exception(void **state) {
    set_mock_state(GRAAL_VM_INIT_FAIL);

    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_deserializer *gsr_deserializer = new_glue_schema_registry_deserializer(p_err);

    assert_null(gsr_deserializer);
    assert_error_and_clear(p_err, "Failed to initialize GraalVM isolate.", ERR_CODE_GRAALVM_INIT_EXCEPTION);

    delete_glue_schema_registry_deserializer(gsr_deserializer);

    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_deletes_instance(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);

    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_deserializer *gsr_deserializer = new_glue_schema_registry_deserializer(p_err);

    assert_non_null(gsr_deserializer);
    delete_glue_schema_registry_deserializer(gsr_deserializer);
    delete_glue_schema_registry_error_holder(p_err);

    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_delete_ignores_NULL_deserializer(void **state) {
    delete_glue_schema_registry_deserializer(NULL);
    assert_true(1);
}

static void test_new_glue_schema_registry_deserializer_delete_ignores_tear_down_failure(void **state) {
    set_mock_state(TEAR_DOWN_FAIL);

    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);
    delete_glue_schema_registry_deserializer(deserializer);

    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_decodes_successfully(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);

    read_only_byte_array *arr = get_read_only_byte_array_fixture();

    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);

    mutable_byte_array *mut_byte_array = glue_schema_registry_deserializer_decode(
            deserializer,
            arr,
            NULL
    );

    mutable_byte_array *expected = get_mut_byte_array_fixture();

    assert_non_null(mut_byte_array);
    assert_int_equal(expected->max_len, mut_byte_array->max_len);
    assert_mutable_byte_array_eq(*expected, *mut_byte_array);

    delete_glue_schema_registry_deserializer(deserializer);
    delete_read_only_byte_array(arr);
    delete_mutable_byte_array(mut_byte_array);
    delete_mutable_byte_array(expected);
    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_decode_throws_exception(void **state) {
    set_mock_state(DECODE_FAILURE);

    read_only_byte_array *arr = get_read_only_byte_array_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);

    mutable_byte_array *mut_byte_array = glue_schema_registry_deserializer_decode(
            deserializer,
            arr,
            p_err
    );

    assert_null(mut_byte_array);
    assert_error_and_clear(p_err, "Decoding failed", ERR_CODE_RUNTIME_ERROR);

    delete_glue_schema_registry_deserializer(deserializer);
    delete_read_only_byte_array(arr);
    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_decode_deserializer_null_throws_exception(void **state) {
    read_only_byte_array *arr = get_read_only_byte_array_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    mutable_byte_array *mutableByteArray = glue_schema_registry_deserializer_decode(
            NULL,
            arr,
            p_err
    );

    assert_null(mutableByteArray);
    assert_error_and_clear(p_err, "Deserializer instance or instance context is null.", ERR_CODE_INVALID_STATE);

    delete_read_only_byte_array(arr);
}

static void test_new_glue_schema_registry_deserializer_decode_arr_null_throws_exception(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);

    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);

    mutable_byte_array *mutableByteArray = glue_schema_registry_deserializer_decode(
            deserializer,
            NULL,
            p_err
    );

    assert_null(mutableByteArray);
    assert_error_and_clear(p_err, "Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);

    delete_glue_schema_registry_deserializer(deserializer);

    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_decodes_schema_successfully(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);

    read_only_byte_array *arr = get_read_only_byte_array_fixture();

    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);

    glue_schema_registry_schema *schema = glue_schema_registry_deserializer_decode_schema(
            deserializer,
            arr,
            NULL
    );

    glue_schema_registry_schema *expected = get_gsr_schema_fixture();

    assert_non_null(schema);
    assert_gsr_schema(*expected, *schema);

    delete_glue_schema_registry_deserializer(deserializer);
    delete_glue_schema_registry_schema(schema);
    delete_glue_schema_registry_schema(expected);
    delete_read_only_byte_array(arr);

    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_decode_schema_throws_exception(void **state) {
    set_mock_state(DECODE_SCHEMA_FAILURE);

    read_only_byte_array *arr = get_read_only_byte_array_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);

    glue_schema_registry_schema *schema = glue_schema_registry_deserializer_decode_schema(
            deserializer,
            arr,
            p_err
    );

    assert_null(schema);
    assert_error_and_clear(p_err, "Decoding schema failed", ERR_CODE_RUNTIME_ERROR);

    delete_glue_schema_registry_deserializer(deserializer);
    delete_read_only_byte_array(arr);
    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_decode_schema_deserializer_null_throws_exception(void **state) {
    read_only_byte_array *arr = get_read_only_byte_array_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    glue_schema_registry_schema *schema = glue_schema_registry_deserializer_decode_schema(
            NULL,
            arr,
            p_err
    );

    assert_null(schema);
    assert_error_and_clear(p_err, "Deserializer instance or instance context is null.", ERR_CODE_INVALID_STATE);

    delete_read_only_byte_array(arr);
}

static void test_new_glue_schema_registry_deserializer_decode_schema_arr_null_throws_exception(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);

    glue_schema_registry_schema *schema = glue_schema_registry_deserializer_decode_schema(
            deserializer,
            NULL,
            p_err
    );

    assert_null(schema);
    assert_error_and_clear(p_err, "Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);

    delete_glue_schema_registry_deserializer(deserializer);

    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_can_decode_successfully(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    read_only_byte_array *arr = get_read_only_byte_array_fixture();

    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);

    bool can_decode = glue_schema_registry_deserializer_can_decode(
            deserializer,
            arr,
            NULL
    );

    assert_true(can_decode);
    delete_glue_schema_registry_deserializer(deserializer);
    delete_read_only_byte_array(arr);

    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_can_decode_throws_exception(void **state) {
    set_mock_state(CAN_DECODE_FAILURE);

    read_only_byte_array *arr = get_read_only_byte_array_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);

    bool can_decode = glue_schema_registry_deserializer_can_decode(
            deserializer,
            arr,
            p_err
    );

    assert_false(can_decode);
    assert_error_and_clear(p_err, "Can decode failed", ERR_CODE_RUNTIME_ERROR);

    delete_glue_schema_registry_deserializer(deserializer);
    delete_read_only_byte_array(arr);
    clear_mock_state();
}

static void test_new_glue_schema_registry_deserializer_can_decode_deserializer_null_throws_exception(void **state) {
    read_only_byte_array *arr = get_read_only_byte_array_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    bool can_decode = glue_schema_registry_deserializer_can_decode(
            NULL,
            arr,
            p_err
    );

    assert_false(can_decode);
    assert_error_and_clear(p_err, "Deserializer instance or instance context is null.", ERR_CODE_INVALID_STATE);

    delete_read_only_byte_array(arr);
}

static void test_new_glue_schema_registry_deserializer_can_decode_arr_null_throws_exception(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL);

    bool can_decode = glue_schema_registry_deserializer_can_decode(
            deserializer,
            NULL,
            p_err
    );

    assert_false(can_decode);
    assert_error_and_clear(p_err, "Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);

    delete_glue_schema_registry_deserializer(deserializer);

    clear_mock_state();
}

int main(void) {
    const struct CMUnitTest tests[] = {
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_created_successfully),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_init_fails_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_deletes_instance),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_delete_ignores_NULL_deserializer),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_delete_ignores_tear_down_failure),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_decodes_successfully),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_decode_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_decode_deserializer_null_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_decode_arr_null_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_decodes_schema_successfully),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_decode_schema_deserializer_null_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_decode_schema_arr_null_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_decode_schema_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_can_decode_successfully),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_can_decode_deserializer_null_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_can_decode_arr_null_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_deserializer_can_decode_throws_exception)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
