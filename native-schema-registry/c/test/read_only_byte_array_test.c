#include <string.h>
#include "cmocka.h"
#include "../include/read_only_byte_array.h"
#include "glue_schema_registry_test_helper.h"

const char * test_array_payload = "🦀HelloWorld!!!!!🥶";

static unsigned char * create_test_payload() {
    size_t len = strlen(test_array_payload);
    unsigned char * data = malloc(len + 1);
    for (int i = 0 ; i <= len ; i ++) {
        data[i] = test_array_payload[i];
    }
    return data;
}

static void assert_read_only_byte_array_eq(read_only_byte_array expected, read_only_byte_array actual) {
    assert_int_equal(expected.len, actual.len);

    assert_memory_equal(expected.data, actual.data, expected.len);
}

static void byte_array_cleanup(read_only_byte_array * byte_array, unsigned char * data) {
    free(data);
    delete_read_only_byte_array(byte_array);
}

static void read_only_byte_array_test_creates_byte_array(void **state) {
    size_t len = strlen(test_array_payload);
    unsigned char * data = create_test_payload();
    glue_schema_registry_error **p_err = create_gsr_error_p_holder();

    read_only_byte_array * byte_array = new_read_only_byte_array(data, len, p_err);

    read_only_byte_array expected;
    expected.data = data;
    expected.len = len;

    assert_read_only_byte_array_eq(expected, *byte_array);
    assert_null(*p_err);

    byte_array_cleanup(byte_array, data);
    cleanup_error(p_err);
}

static void read_only_byte_array_test_returns_NULL_when_data_len_is_Zero(void **state) {
    glue_schema_registry_error **p_err = create_gsr_error_p_holder();
    unsigned char * data = create_test_payload();
    read_only_byte_array *actual = new_read_only_byte_array(data, 0, p_err);

    assert_null(actual);
    assert_non_null(*p_err);
    glue_schema_registry_error *err = *p_err;
    assert_int_equal(err->code, ERR_CODE_NULL_PARAMETERS);
    assert_string_equal(err->msg, "Data is NULL or is of zero-length");

    cleanup_error(p_err);
    byte_array_cleanup(actual, data);
}

static void read_only_byte_array_test_returns_NULL_when_data_is_NULL(void **state) {
    glue_schema_registry_error **p_err = create_gsr_error_p_holder();
    read_only_byte_array *actual = new_read_only_byte_array(NULL, 10, p_err);

    assert_null(actual);
    assert_non_null(*p_err);
    glue_schema_registry_error *err = *p_err;
    assert_int_equal(err->code, ERR_CODE_NULL_PARAMETERS);
    assert_string_equal(err->msg, "Data is NULL or is of zero-length");

    cleanup_error(p_err);
}

static void read_only_byte_array_test_deletes_byte_array(void **state) {
    glue_schema_registry_error **p_err = create_gsr_error_p_holder();
    size_t len = strlen(test_array_payload);
    unsigned char * data = create_test_payload();
    read_only_byte_array *byte_array = new_read_only_byte_array(data, len, p_err);

    cleanup_error(p_err);
    delete_read_only_byte_array(byte_array);
    free(data);
}

static void read_only_byte_array_get_data_fetches_bytes_correctly(void **state) {
    glue_schema_registry_error **p_err = create_gsr_error_p_holder();
    size_t len = strlen(test_array_payload);
    unsigned char * expected = create_test_payload();

    read_only_byte_array * byte_array = new_read_only_byte_array(expected, len, p_err);

    unsigned char * actual = read_only_byte_array_get_data(byte_array);
    size_t actual_len = read_only_byte_array_get_len(byte_array);

    assert_int_equal(len, actual_len);
    assert_memory_equal(expected, actual, len);

    cleanup_error(p_err);
    byte_array_cleanup(byte_array, expected);
}

static void read_only_byte_array_test_does_not_fail_when_error_pointer_is_null(void **state) {
    size_t len = strlen(test_array_payload);
    unsigned char * expected = create_test_payload();

    read_only_byte_array * byte_array = new_read_only_byte_array(expected, len, NULL);
    assert_non_null(byte_array);
    byte_array_cleanup(byte_array, expected);
}

int main() {
    const struct CMUnitTest tests[] = {
            cmocka_unit_test(read_only_byte_array_test_creates_byte_array),
            cmocka_unit_test(read_only_byte_array_test_deletes_byte_array),
            cmocka_unit_test(read_only_byte_array_get_data_fetches_bytes_correctly),
            cmocka_unit_test(read_only_byte_array_test_returns_NULL_when_data_is_NULL),
            cmocka_unit_test(read_only_byte_array_test_returns_NULL_when_data_len_is_Zero),
            cmocka_unit_test(read_only_byte_array_test_does_not_fail_when_error_pointer_is_null)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
