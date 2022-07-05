#ifndef NATIVE_SCHEMA_REGISTRY_GLUE_SCHEMA_REGISTRY_TEST_HELPER_H
#define NATIVE_SCHEMA_REGISTRY_GLUE_SCHEMA_REGISTRY_TEST_HELPER_H
#include "../include/glue_schema_registry_error.h"

glue_schema_registry_error ** create_gsr_error_p_holder(void);

void cleanup_error(glue_schema_registry_error **p_err);

#endif //NATIVE_SCHEMA_REGISTRY_GLUE_SCHEMA_REGISTRY_TEST_HELPER_H
