#include <string.h>
#include "glue_schema_registry_serializer.h"
#include "graal_isolate.h"
#include "cmocka.h"
#include "libnativeschemaregistry_mock.h"
#include "glue_schema_registry_test_helper.h"

static void test_new_glue_schema_registry_serializer_created_successfully(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    glue_schema_registry_serializer *gsr_serializer = new_glue_schema_registry_serializer(NULL, NULL);

    assert_non_null(gsr_serializer);
    assert_non_null(gsr_serializer->instance_context);
    assert_int_equal(sizeof(graal_isolatethread_t *), sizeof(gsr_serializer->instance_context));
    delete_glue_schema_registry_serializer(gsr_serializer);

    clear_mock_state();
}

static void test_new_glue_schema_registry_serializer_init_fails_throws_exception(void **state) {
    set_mock_state(GRAAL_VM_INIT_FAIL);

    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_serializer *gsr_serializer = new_glue_schema_registry_serializer(NULL, p_err);

    assert_null(gsr_serializer);
    assert_error_and_clear(p_err, "Failed to initialize GraalVM isolate.", ERR_CODE_GRAALVM_INIT_EXCEPTION);

    delete_glue_schema_registry_serializer(gsr_serializer);

    clear_mock_state();
}

static void test_new_glue_schema_registry_serializer_config_init_fails_throws_exception(void **state) {
    set_mock_state(CONFIG_INIT_SERIALIZER_FAIL);

    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_serializer *gsr_serializer = new_glue_schema_registry_serializer(NULL, p_err);

    assert_null(gsr_serializer);
    assert_error_and_clear(p_err, "Failed to initialize serializer with configuration file.", ERR_CODE_RUNTIME_ERROR);

    delete_glue_schema_registry_serializer(gsr_serializer);

    clear_mock_state();
}

static void test_new_glue_schema_registry_serializer_deletes_instance(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);

    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_serializer *gsr_serializer = new_glue_schema_registry_serializer(NULL, p_err);

    assert_non_null(gsr_serializer);
    delete_glue_schema_registry_serializer(gsr_serializer);
    delete_glue_schema_registry_error_holder(p_err);

    clear_mock_state();
}

static void test_new_glue_schema_registry_serializer_delete_ignores_NULL_serializer(void **state) {
    delete_glue_schema_registry_serializer(NULL);
    assert_true(1);
}

static void test_new_glue_schema_registry_serializer_delete_ignores_tear_down_failure(void **state) {
    set_mock_state(TEAR_DOWN_FAIL);
    glue_schema_registry_serializer *serializer = new_glue_schema_registry_serializer(NULL, NULL);
    delete_glue_schema_registry_serializer(serializer);

    clear_mock_state();
}


static void test_new_glue_schema_registry_serializer_encodes_successfully(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    const char *transport_name = get_transport_name_fixture();
    glue_schema_registry_schema *schema = get_gsr_schema_fixture();
    read_only_byte_array *arr = get_read_only_byte_array_fixture();

    glue_schema_registry_serializer *serializer = new_glue_schema_registry_serializer(NULL, NULL);

    mutable_byte_array *mut_byte_array = glue_schema_registry_serializer_encode(
            serializer,
            arr,
            transport_name,
            schema,
            NULL
    );

    mutable_byte_array *expected = get_mut_byte_array_fixture();

    assert_non_null(mut_byte_array);
    assert_int_equal(expected->max_len, mut_byte_array->max_len);
    assert_mutable_byte_array_eq(*expected, *mut_byte_array);

    delete_glue_schema_registry_serializer(serializer);
    delete_glue_schema_registry_schema(schema);
    delete_read_only_byte_array(arr);
    delete_mutable_byte_array(mut_byte_array);
    delete_mutable_byte_array(expected);
    clear_mock_state();
}

static void test_new_glue_schema_registry_serializer_encode_throws_exception(void **state) {
    set_mock_state(ENCODE_FAILURE);
    const char *transport_name = get_transport_name_fixture();
    glue_schema_registry_schema *schema = get_gsr_schema_fixture();
    read_only_byte_array *arr = get_read_only_byte_array_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    glue_schema_registry_serializer *serializer = new_glue_schema_registry_serializer(NULL, NULL);

    mutable_byte_array *mut_byte_array = glue_schema_registry_serializer_encode(
            serializer,
            arr,
            transport_name,
            schema,
            p_err
    );

    assert_null(mut_byte_array);
    assert_error_and_clear(p_err, "Encoding failed", ERR_CODE_RUNTIME_ERROR);

    delete_glue_schema_registry_serializer(serializer);
    delete_glue_schema_registry_schema(schema);
    delete_read_only_byte_array(arr);
    clear_mock_state();
}

static void test_new_glue_schema_registry_serializer_encode_serializer_null_throws_exception(void **state) {
    const char *transport_name = get_transport_name_fixture();
    glue_schema_registry_schema *schema = get_gsr_schema_fixture();
    read_only_byte_array *arr = get_read_only_byte_array_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    mutable_byte_array *mutableByteArray = glue_schema_registry_serializer_encode(
            NULL,
            arr,
            transport_name,
            schema,
            p_err
    );

    assert_null(mutableByteArray);
    assert_error_and_clear(p_err, "Serializer instance or instance context is null.", ERR_CODE_INVALID_STATE);

    delete_glue_schema_registry_schema(schema);
    delete_read_only_byte_array(arr);
}

static void test_new_glue_schema_registry_serializer_encode_schema_null_throws_exception(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    const char *transport_name = get_transport_name_fixture();
    read_only_byte_array *arr = get_read_only_byte_array_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_serializer *serializer = new_glue_schema_registry_serializer(NULL, NULL);

    mutable_byte_array *mutableByteArray = glue_schema_registry_serializer_encode(
            serializer,
            arr,
            transport_name,
            NULL,
            p_err
    );

    assert_null(mutableByteArray);
    assert_error_and_clear(p_err, "Schema passed cannot be null", ERR_CODE_NULL_PARAMETERS);

    delete_glue_schema_registry_serializer(serializer);
    delete_read_only_byte_array(arr);

    clear_mock_state();
}

static void test_new_glue_schema_registry_serializer_encode_arr_null_throws_exception(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    const char *transport_name = get_transport_name_fixture();
    glue_schema_registry_schema *schema = get_gsr_schema_fixture();
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    glue_schema_registry_serializer *serializer = new_glue_schema_registry_serializer(NULL, NULL);

    mutable_byte_array *mutableByteArray = glue_schema_registry_serializer_encode(
            serializer,
            NULL,
            transport_name,
            schema,
            p_err
    );

    assert_null(mutableByteArray);
    assert_error_and_clear(p_err, "Byte array cannot be null", ERR_CODE_NULL_PARAMETERS);

    delete_glue_schema_registry_serializer(serializer);
    delete_glue_schema_registry_schema(schema);

    clear_mock_state();
}

static void test_serializer_encode_attach_thread_failure(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    glue_schema_registry_serializer *gsr_serializer = new_glue_schema_registry_serializer(NULL, NULL);

    set_mock_state(ATTACH_THREAD_FAIL);

    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    mutable_byte_array *array = new_mutable_byte_array(10, NULL);
    glue_schema_registry_schema *schema = new_glue_schema_registry_schema("test", "def", "AVRO", NULL, NULL);

    mutable_byte_array *result = glue_schema_registry_serializer_encode(gsr_serializer, array, "test", schema, p_err);

    assert_null(result);
    assert_error_and_clear(p_err, "Failed to attach thread to GraalVM isolate", ERR_CODE_GRAAL_ATTACH_FAILED);

    delete_mutable_byte_array(array);
    delete_glue_schema_registry_schema(schema);
    delete_glue_schema_registry_serializer(gsr_serializer);
    clear_mock_state();
}

static void test_serializer_encode_detach_thread_failure(void **state) {
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    glue_schema_registry_serializer *gsr_serializer = new_glue_schema_registry_serializer(NULL, NULL);

    set_mock_state(DETACH_THREAD_FAIL);

    mutable_byte_array *array = new_mutable_byte_array(10, NULL);
    glue_schema_registry_schema *schema = new_glue_schema_registry_schema("test", "def", "AVRO", NULL, NULL);

    // Should succeed but log warning (detach failure doesn't prevent operation success)
    mutable_byte_array *result = glue_schema_registry_serializer_encode(gsr_serializer, array, "test", schema, NULL);

    delete_mutable_byte_array(array);
    delete_glue_schema_registry_schema(schema);
    if (result) delete_mutable_byte_array(result);
    delete_glue_schema_registry_serializer(gsr_serializer);
    clear_mock_state();
}


int main(void) {
    const struct CMUnitTest tests[] = {
            cmocka_unit_test(test_new_glue_schema_registry_serializer_created_successfully),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_init_fails_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_config_init_fails_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_deletes_instance),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_delete_ignores_NULL_serializer),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_delete_ignores_tear_down_failure),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_encodes_successfully),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_encode_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_encode_serializer_null_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_encode_schema_null_throws_exception),
            cmocka_unit_test(test_new_glue_schema_registry_serializer_encode_arr_null_throws_exception),
            cmocka_unit_test(test_serializer_encode_attach_thread_failure),
            cmocka_unit_test(test_serializer_encode_detach_thread_failure)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
