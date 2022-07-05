#include <stdlib.h>
#include "glue_schema_registry_test_helper.h"

glue_schema_registry_error ** create_gsr_error_p_holder() {
    return (glue_schema_registry_error **) malloc(sizeof(glue_schema_registry_error *));
}

void cleanup_error(glue_schema_registry_error **p_err) {
    delete_glue_schema_registry_error(*p_err);
    free(p_err);
}
