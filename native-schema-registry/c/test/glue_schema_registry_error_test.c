#include "../include/glue_schema_registry_error.h"
#include <stdlib.h>
#include "cmocka.h"

#define TEST_ERROR_MSG "Some error occurred"
#define TEST_ERROR_CODE 11

static void error_cleanup(glue_schema_registry_error * obj) {
    delete_glue_schema_registry_error(obj);
}

static void glue_schema_registry_error_creates_new_glue_schema_registry_error(void **state) {
    glue_schema_registry_error *error = new_glue_schema_registry_error(TEST_ERROR_MSG, TEST_ERROR_CODE);
    assert_non_null(error);

    assert_string_equal(TEST_ERROR_MSG, error->msg);
    assert_int_equal(TEST_ERROR_CODE, error->code);
    error_cleanup(error);
}

static void glue_schema_registry_error_deletes_the_instance(void **state) {
    glue_schema_registry_error *error = new_glue_schema_registry_error(TEST_ERROR_MSG, TEST_ERROR_CODE);

    delete_glue_schema_registry_error(error);
}

static void glue_schema_registry_error_returns_null_when_null_msg_is_passed(void **state) {
    glue_schema_registry_error *error = new_glue_schema_registry_error(NULL, TEST_ERROR_CODE);
    assert_null(error);

    error_cleanup(error);
}

static void glue_schema_registry_error_sets_exception_instance_as_expected(void **state) {
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    throw_error(p_err, TEST_ERROR_MSG, TEST_ERROR_CODE);

    assert_non_null(p_err);
    glue_schema_registry_error *err = *p_err;
    assert_non_null(err);

    assert_string_equal(TEST_ERROR_MSG, err->msg);
    assert_int_equal(TEST_ERROR_CODE, err->code);

    delete_glue_schema_registry_error_holder(p_err);
}

static void glue_schema_registry_error_does_not_set_exception_if_error_pointer_is_null(void **state) {
    throw_error(NULL, TEST_ERROR_MSG, TEST_ERROR_CODE);
    assert_true(1);
}

static void glue_schema_registry_error_gets_error_message_of_requested_size(void **state) {
    glue_schema_registry_error *error = new_glue_schema_registry_error(TEST_ERROR_MSG, TEST_ERROR_CODE);
    char msg[1000];
    glue_schema_registry_error_get_msg(error, msg, sizeof msg);

    assert_string_equal(TEST_ERROR_MSG, msg);
    error_cleanup(error);
}

static void glue_schema_registry_error_truncates_error_message_when_len_is_exceeded(void **state) {
    glue_schema_registry_error *error = new_glue_schema_registry_error(TEST_ERROR_MSG, TEST_ERROR_CODE);
    char msg[10];
    glue_schema_registry_error_get_msg(error, msg, sizeof msg);

    assert_string_not_equal(TEST_ERROR_MSG, msg);
    assert_string_equal("Some erro", msg);
    error_cleanup(error);
}

static void glue_schema_registry_error_truncates_error_message_when_len_matches_msg_len(void **state) {
    glue_schema_registry_error *error = new_glue_schema_registry_error(TEST_ERROR_MSG, TEST_ERROR_CODE);
    char msg[20];
    glue_schema_registry_error_get_msg(error, msg, sizeof msg);

    assert_string_equal(TEST_ERROR_MSG, msg);
    error_cleanup(error);
}

static void glue_schema_registry_error_holder_creates_new_instance(void **state) {
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    assert_non_null(p_err);

    delete_glue_schema_registry_error_holder(p_err);
}

static void glue_schema_registry_error_holder_creates_deletes_instance(void **state) {
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    delete_glue_schema_registry_error_holder(p_err);
}

int main() {

    const struct CMUnitTest tests[] = {
            cmocka_unit_test(glue_schema_registry_error_deletes_the_instance),
            cmocka_unit_test(glue_schema_registry_error_returns_null_when_null_msg_is_passed),
            cmocka_unit_test(glue_schema_registry_error_sets_exception_instance_as_expected),
            cmocka_unit_test(glue_schema_registry_error_does_not_set_exception_if_error_pointer_is_null),
            cmocka_unit_test(glue_schema_registry_error_creates_new_glue_schema_registry_error),
            cmocka_unit_test(glue_schema_registry_error_gets_error_message_of_requested_size),
            cmocka_unit_test(glue_schema_registry_error_truncates_error_message_when_len_is_exceeded),
            cmocka_unit_test(glue_schema_registry_error_truncates_error_message_when_len_matches_msg_len),
            cmocka_unit_test(glue_schema_registry_error_holder_creates_new_instance),
            cmocka_unit_test(glue_schema_registry_error_holder_creates_deletes_instance)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
