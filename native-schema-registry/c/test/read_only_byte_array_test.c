#include <string.h>
#include "cmocka.h"
#include "../include/read_only_byte_array.h"

const char * test_array_payload = "🦀HelloWorld!!!!!🥶";

unsigned char * create_test_payload() {
    size_t len = strlen(test_array_payload);
    unsigned char * data = malloc(len + 1);
    for (int i = 0 ; i <= len ; i ++) {
        data[i] = test_array_payload[i];
    }
    return data;
}

void assert_read_only_byte_array_eq(read_only_byte_array expected, read_only_byte_array actual) {
    assert_int_equal(expected.len, actual.len);

    assert_memory_equal(expected.data, actual.data, expected.len);
}

void byte_array_cleanup(read_only_byte_array * byte_array, unsigned char * data) {
    free(data);
    delete_read_only_byte_array(byte_array);
}

static void read_only_byte_array_test_creates_byte_array(void **state) {
    size_t len = strlen(test_array_payload);
    unsigned char * data = create_test_payload();

    read_only_byte_array * byte_array = new_read_only_byte_array(data, len);

    read_only_byte_array expected;
    expected.data = data;
    expected.len = len;

    assert_read_only_byte_array_eq(expected, *byte_array);
    byte_array_cleanup(byte_array, data);
}

static void read_only_byte_array_test_returns_NULL_when_data_is_NULL_OR_empty(void **state) {
    read_only_byte_array *actual = new_read_only_byte_array(NULL, 10);
    assert_null(actual);

    unsigned char * data = create_test_payload();
    actual = new_read_only_byte_array(data, 0);
    assert_null(actual);

    byte_array_cleanup(actual, data);
}

static void read_only_byte_array_test_deletes_byte_array(void **state) {
    size_t len = strlen(test_array_payload);
    unsigned char * data = create_test_payload();
    read_only_byte_array *byte_array = new_read_only_byte_array(data, len);

    delete_read_only_byte_array(byte_array);
    free(data);
}

void read_only_byte_array_get_data_fetches_bytes_correctly(void **state) {
    size_t len = strlen(test_array_payload);
    unsigned char * expected = create_test_payload();

    read_only_byte_array * byte_array = new_read_only_byte_array(expected, len);

    unsigned char * actual = read_only_byte_array_get_data(byte_array);
    size_t actual_len = read_only_byte_array_get_len(byte_array);

    assert_int_equal(len, actual_len);
    assert_memory_equal(expected, actual, len);

    byte_array_cleanup(byte_array, expected);
}

int main() {
    const struct CMUnitTest tests[] = {
            cmocka_unit_test(read_only_byte_array_test_creates_byte_array),
            cmocka_unit_test(read_only_byte_array_test_deletes_byte_array),
            cmocka_unit_test(read_only_byte_array_get_data_fetches_bytes_correctly),
            cmocka_unit_test(read_only_byte_array_test_returns_NULL_when_data_is_NULL_OR_empty)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
