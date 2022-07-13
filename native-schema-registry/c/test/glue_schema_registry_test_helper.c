#include <stdlib.h>
#include <string.h>
#include "glue_schema_registry_test_helper.h"
#include "cmocka.h"

glue_schema_registry_schema * get_gsr_schema_fixture(void) {
    return new_glue_schema_registry_schema("SomeNameOfSchema", "Some Def {}", "PROTOBUF", NULL);
}

read_only_byte_array * get_read_only_byte_array_fixture(void) {
    unsigned char payload[24];
    return new_read_only_byte_array(payload, sizeof(payload), NULL);
}

mutable_byte_array * get_mut_byte_array_fixture(void) {
    const char * test_data = "Test data to write 🦷👑";
    size_t len = strlen(test_data) + 1;
    return new_mutable_byte_array(len, NULL);
}

void assert_mutable_byte_array_eq(mutable_byte_array expected, mutable_byte_array actual) {
    assert_int_equal(expected.max_len, actual.max_len);

    //Ensure the pointers are not pointing to same memory address
    assert_ptr_not_equal(expected.data, actual.data);

    for (size_t index = 0; index < expected.max_len; index++) {
        assert_int_equal(expected.data[index], actual.data[index]);
    }
}

const char * get_transport_name_fixture(void) {
    return "Some transport name 123✅";
}

void assert_error_and_clear(glue_schema_registry_error **p_err, const char *msg, int code) {
    assert_non_null(p_err);
    glue_schema_registry_error *err = *p_err;

    assert_non_null(err);
    assert_int_equal(code, err->code);
    assert_string_equal(msg, err->msg);

    delete_glue_schema_registry_error_holder(p_err);
}

void assert_gsr_schema(glue_schema_registry_schema expected, glue_schema_registry_schema actual) {
    assert_string_equal(expected.schema_name, actual.schema_name);
    assert_string_equal(expected.schema_def, actual.schema_def);
    assert_string_equal(expected.data_format, actual.data_format);
}
