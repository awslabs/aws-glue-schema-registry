#include <string.h>
#include "cmocka.h"
#include "../include/mutable_byte_array.h"

const char * test_mutable_array_payload = "Foobar 01-23 1231!!!!!🍎aosmd🦷";

unsigned char * create_mutable_test_payload() {
    size_t len = strlen(test_mutable_array_payload);
    unsigned char * data = malloc(len + 1);
    for (int i = 0 ; i <= len ; i ++) {
        data[i] = test_mutable_array_payload[i];
    }
    return data;
}

void assert_mutable_byte_array_eq(mutable_byte_array expected, mutable_byte_array actual) {
    assert_int_equal(expected.max_len, actual.max_len);

    //Ensure the pointers are not pointing to same memory address
    assert_ptr_not_equal(expected.data, actual.data);

    for (size_t index = 0; index < expected.max_len; index++) {
        assert_int_equal(expected.data[index], actual.data[index]);
    }
}

void assert_mutable_byte_array_data_zero(mutable_byte_array *byte_array) {
    for (size_t index = 0; index < byte_array->max_len; index++) {
        assert_int_equal(byte_array->data[index], 0);
    }
}

void mutable_byte_array_cleanup(mutable_byte_array * byte_array, unsigned char * data) {
    if (data != NULL) {
        free(data);
    }
    delete_mutable_byte_array(byte_array);
}

static void mutable_byte_array_test_creates_byte_array(void **state) {
    size_t len = strlen(test_mutable_array_payload);

    mutable_byte_array * byte_array = new_mutable_byte_array(len);

    mutable_byte_array expected;
    expected.max_len = len;
    expected.data = calloc(len, sizeof(unsigned  char));

    assert_mutable_byte_array_eq(expected, *byte_array);

    assert_mutable_byte_array_data_zero(byte_array);
    mutable_byte_array_cleanup(byte_array, expected.data);
}

static void mutable_byte_array_test_returns_NULL_when_initialized_empty(void **state) {
    mutable_byte_array *actual = new_mutable_byte_array(0);
    assert_null(actual);
}

static void mutable_byte_array_test_writes_byte_array(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    mutable_byte_array *byte_array = new_mutable_byte_array(len);
    for (size_t index = 0; index < len; index++) {
        mutable_byte_array_write(byte_array, index, test_mutable_array_payload[index]);
    }

    mutable_byte_array expected;
    expected.data = create_mutable_test_payload();
    expected.max_len = len;

    assert_mutable_byte_array_eq(expected, *byte_array);
    mutable_byte_array_cleanup(byte_array, expected.data);
}

static void mutable_byte_array_test_noop_when_array_is_NULL(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    mutable_byte_array *byte_array = new_mutable_byte_array(len);
    assert_mutable_byte_array_data_zero(byte_array);

    size_t index = 4;
    mutable_byte_array_write(NULL, index, test_mutable_array_payload[index]);

    assert_mutable_byte_array_data_zero(byte_array);
    assert_int_equal(len, byte_array->max_len);
    mutable_byte_array_cleanup(byte_array, NULL);
}

static void mutable_byte_array_test_noop_when_index_is_out_of_range(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    mutable_byte_array *byte_array = new_mutable_byte_array(len);
    assert_mutable_byte_array_data_zero(byte_array);

    size_t valid_index = 2;
    size_t index_gt_max_len = len;
    mutable_byte_array_write(byte_array, index_gt_max_len, test_mutable_array_payload[valid_index]);

    //assertions
    assert_int_equal(len, byte_array->max_len);
    assert_mutable_byte_array_data_zero(byte_array);

    index_gt_max_len = len + 4;
    mutable_byte_array_write(byte_array, index_gt_max_len, test_mutable_array_payload[valid_index]);

    //assertions
    assert_int_equal(len, byte_array->max_len);
    assert_mutable_byte_array_data_zero(byte_array);

    //Cleanup
    mutable_byte_array_cleanup(byte_array, NULL);
}

static void mutable_byte_array_test_gets_data_as_expected(void **state) {
    size_t len = strlen(test_mutable_array_payload) + 1;
    mutable_byte_array *byte_array = new_mutable_byte_array(len);

    for (size_t index = 0; index < len; index++) {
        mutable_byte_array_write(byte_array, index, test_mutable_array_payload[index]);
    }

    unsigned char * data = mutable_byte_array_get_data(byte_array);

    assert_string_equal(test_mutable_array_payload, data);
    assert_ptr_not_equal(test_mutable_array_payload, data);

    mutable_byte_array_cleanup(byte_array, NULL);
}

static void mutable_byte_array_test_gets_max_len_as_expected(void **state) {
    size_t len = strlen(test_mutable_array_payload) + 1;
    mutable_byte_array *byte_array = new_mutable_byte_array(len);

    assert_int_equal(len, mutable_byte_array_get_max_len(byte_array));

    //Populate the byte array
    for (size_t index = 0; index < len; index++) {
        mutable_byte_array_write(byte_array, index, test_mutable_array_payload[index]);
    }

    //Assert again
    assert_int_equal(len, mutable_byte_array_get_max_len(byte_array));

    mutable_byte_array_cleanup(byte_array, NULL);
}

static void mutable_byte_array_test_deletes_byte_array(void **state) {
    size_t len = strlen(test_mutable_array_payload);
    mutable_byte_array *byte_array = new_mutable_byte_array(len);
    for (size_t index = 0; index < len; index++) {
        mutable_byte_array_write(byte_array, index, test_mutable_array_payload[index]);
    }

    delete_mutable_byte_array(byte_array);
}

int main() {
    const struct CMUnitTest tests[] = {
            cmocka_unit_test(mutable_byte_array_test_creates_byte_array),
            cmocka_unit_test(mutable_byte_array_test_returns_NULL_when_initialized_empty),
            cmocka_unit_test(mutable_byte_array_test_deletes_byte_array),
            cmocka_unit_test(mutable_byte_array_test_writes_byte_array),
            cmocka_unit_test(mutable_byte_array_test_noop_when_index_is_out_of_range),
            cmocka_unit_test(mutable_byte_array_test_gets_data_as_expected),
            cmocka_unit_test(mutable_byte_array_test_gets_max_len_as_expected),
            cmocka_unit_test(mutable_byte_array_test_noop_when_array_is_NULL)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
