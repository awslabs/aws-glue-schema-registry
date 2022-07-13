#include <string.h>
#include "cmocka.h"
#include "glue_schema_registry_test_helper.h"
#include "mutable_byte_array.h"

const char * test_mutable_array_payload = "Foobar 01-23 1231!!!!!🍎aosmd🦷";

static unsigned char * create_mutable_test_payload() {
    size_t len = strlen(test_mutable_array_payload);
    unsigned char * data = malloc(len + 1);
    for (int i = 0 ; i <= len ; i ++) {
        data[i] = test_mutable_array_payload[i];
    }
    return data;
}

static void assert_mutable_byte_array_data_zero(mutable_byte_array *byte_array) {
    for (size_t index = 0; index < byte_array->max_len; index++) {
        assert_int_equal(byte_array->data[index], 0);
    }
}

static void mutable_byte_array_cleanup(mutable_byte_array * byte_array, unsigned char * data) {
    if (data != NULL) {
        free(data);
    }
    delete_mutable_byte_array(byte_array);
}

static void mutable_byte_array_test_creates_byte_array(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    glue_schema_registry_error ** p_err = new_glue_schema_registry_error_holder();

    mutable_byte_array * byte_array = new_mutable_byte_array(len, p_err);

    mutable_byte_array expected;
    expected.max_len = len;
    expected.data = calloc(len, sizeof(unsigned  char));

    assert_mutable_byte_array_eq(expected, *byte_array);

    assert_mutable_byte_array_data_zero(byte_array);
    glue_schema_registry_error * err = *p_err;

    //No errors are set.
    assert_null(err);
    delete_glue_schema_registry_error_holder(p_err);

    mutable_byte_array_cleanup(byte_array, expected.data);
}

static void mutable_byte_array_test_returns_NULL_when_initialized_empty(void **state) {
    glue_schema_registry_error ** p_err = new_glue_schema_registry_error_holder();
    mutable_byte_array *actual = new_mutable_byte_array(0, p_err);
    assert_null(actual);
    assert_non_null(p_err);

    glue_schema_registry_error * err = *p_err;
    assert_non_null(err);
    assert_int_equal(err->code, ERR_CODE_INVALID_PARAMETERS);
    assert_string_equal(err->msg, "Cannot create byte array of size 0");

    delete_glue_schema_registry_error_holder(p_err);
}

static void mutable_byte_array_test_writes_byte_array(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    mutable_byte_array *byte_array = new_mutable_byte_array(len, p_err);

    for (size_t index = 0; index < len; index++) {
        mutable_byte_array_write(byte_array, index, test_mutable_array_payload[index], p_err);
    }

    mutable_byte_array expected;
    expected.data = create_mutable_test_payload();
    expected.max_len = len;

    assert_mutable_byte_array_eq(expected, *byte_array);
    //No errors are set.
    assert_null(*p_err);
    delete_glue_schema_registry_error_holder(p_err);

    mutable_byte_array_cleanup(byte_array, expected.data);
}

static void mutable_byte_array_test_noop_when_array_is_NULL(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    mutable_byte_array *byte_array = new_mutable_byte_array(len, p_err);
    assert_mutable_byte_array_data_zero(byte_array);

    size_t index = 4;
    mutable_byte_array_write(NULL, index, test_mutable_array_payload[index], p_err);

    assert_mutable_byte_array_data_zero(byte_array);
    assert_int_equal(len, byte_array->max_len);

    glue_schema_registry_error *err = *p_err;
    assert_non_null(err);
    assert_int_equal(err->code, ERR_CODE_NULL_PARAMETERS);
    assert_string_equal(err->msg, "Cannot write to NULL byte array");

    delete_glue_schema_registry_error_holder(p_err);
    mutable_byte_array_cleanup(byte_array, NULL);
}

static void mutable_byte_array_test_throws_exception_when_index_is_out_of_range(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    mutable_byte_array *byte_array = new_mutable_byte_array(len, p_err);
    assert_mutable_byte_array_data_zero(byte_array);

    size_t valid_index = 2;
    size_t index_gt_max_len = len;
    mutable_byte_array_write(byte_array, index_gt_max_len, test_mutable_array_payload[valid_index], p_err);
    glue_schema_registry_error *err = *p_err;
    assert_non_null(err);
    assert_int_equal(err->code, ERR_CODE_INVALID_PARAMETERS);
    assert_string_equal(err->msg, "Index: 35 out of range for the byte-array of max_len: 35");

    //assertions
    assert_int_equal(len, byte_array->max_len);
    assert_mutable_byte_array_data_zero(byte_array);

    //Cleanup
    delete_glue_schema_registry_error_holder(p_err);
    mutable_byte_array_cleanup(byte_array, NULL);
}

static void mutable_byte_array_test_throws_exception_when_index_is_at_range(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    mutable_byte_array *byte_array = new_mutable_byte_array(len, p_err);
    assert_mutable_byte_array_data_zero(byte_array);

    size_t index_gt_max_len = len + 4;
    size_t valid_index = 2;
    mutable_byte_array_write(byte_array, index_gt_max_len, test_mutable_array_payload[valid_index], p_err);

    //assertions
    assert_int_equal(len, byte_array->max_len);
    assert_mutable_byte_array_data_zero(byte_array);

    glue_schema_registry_error *err = *p_err;
    assert_non_null(err);
    assert_int_equal(err->code, ERR_CODE_INVALID_PARAMETERS);
    assert_string_equal(err->msg, "Index: 39 out of range for the byte-array of max_len: 35");

    delete_glue_schema_registry_error_holder(p_err);

    //Cleanup
    mutable_byte_array_cleanup(byte_array, NULL);
}

static void mutable_byte_array_test_gets_data_as_expected(void **state) {
    size_t len = strlen(test_mutable_array_payload) + 1;
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();

    mutable_byte_array *byte_array = new_mutable_byte_array(len, p_err);

    for (size_t index = 0; index < len; index++) {
        mutable_byte_array_write(byte_array, index, test_mutable_array_payload[index], p_err);
    }

    unsigned char * data = mutable_byte_array_get_data(byte_array);

    assert_string_equal(test_mutable_array_payload, data);
    assert_ptr_not_equal(test_mutable_array_payload, data);

    delete_glue_schema_registry_error_holder(p_err);

    mutable_byte_array_cleanup(byte_array, NULL);
}

static void mutable_byte_array_test_gets_max_len_as_expected(void **state) {
    size_t len = strlen(test_mutable_array_payload) + 1;
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    mutable_byte_array *byte_array = new_mutable_byte_array(len, p_err);

    assert_int_equal(len, mutable_byte_array_get_max_len(byte_array));

    //Populate the byte array
    for (size_t index = 0; index < len; index++) {
        mutable_byte_array_write(byte_array, index, test_mutable_array_payload[index], p_err);
    }

    //Assert again
    assert_int_equal(len, mutable_byte_array_get_max_len(byte_array));

    delete_glue_schema_registry_error_holder(p_err);
    mutable_byte_array_cleanup(byte_array, NULL);
}

static void mutable_byte_array_test_deletes_byte_array(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    mutable_byte_array *byte_array = new_mutable_byte_array(len, p_err);
    for (size_t index = 0; index < len; index++) {
        mutable_byte_array_write(byte_array, index, test_mutable_array_payload[index], p_err);
    }

    delete_glue_schema_registry_error_holder(p_err);
    delete_mutable_byte_array(byte_array);
}

static void mutable_byte_array_test_ignores_null_err_pointer(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    mutable_byte_array *byte_array = new_mutable_byte_array(len, NULL);

    assert_non_null(byte_array);

    unsigned int any_byte = 3;
    mutable_byte_array_write(byte_array, 0, any_byte, NULL);
    delete_mutable_byte_array(byte_array);
}

int main(void) {
    const struct CMUnitTest tests[] = {
            cmocka_unit_test(mutable_byte_array_test_creates_byte_array),
            cmocka_unit_test(mutable_byte_array_test_returns_NULL_when_initialized_empty),
            cmocka_unit_test(mutable_byte_array_test_deletes_byte_array),
            cmocka_unit_test(mutable_byte_array_test_writes_byte_array),
            cmocka_unit_test(mutable_byte_array_test_throws_exception_when_index_is_at_range),
            cmocka_unit_test(mutable_byte_array_test_throws_exception_when_index_is_out_of_range),
            cmocka_unit_test(mutable_byte_array_test_gets_data_as_expected),
            cmocka_unit_test(mutable_byte_array_test_gets_max_len_as_expected),
            cmocka_unit_test(mutable_byte_array_test_ignores_null_err_pointer),
            cmocka_unit_test(mutable_byte_array_test_noop_when_array_is_NULL)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
