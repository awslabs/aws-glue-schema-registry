#ifndef GLUE_SCHEMA_REGISTRY_TEST_HELPER_H
#define GLUE_SCHEMA_REGISTRY_TEST_HELPER_H
#include "read_only_byte_array.h"
#include "mutable_byte_array.h"
#include "glue_schema_registry_schema.h"

glue_schema_registry_schema * get_gsr_schema_fixture(void);

read_only_byte_array * get_read_only_byte_array_fixture(void);

mutable_byte_array * get_mut_byte_array_fixture(void);

void assert_mutable_byte_array_eq(mutable_byte_array expected, mutable_byte_array actual);

void assert_error_and_clear(glue_schema_registry_error **p_err, const char *msg, int code);

void assert_gsr_schema(glue_schema_registry_schema, glue_schema_registry_schema);

const char * get_transport_name_fixture(void);

#endif /* GLUE_SCHEMA_REGISTRY_TEST_HELPER_H */
