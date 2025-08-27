%module GsrSerDe
%{
#include "glue_schema_registry_error.h"
#include <stdlib.h>
#include <stddef.h>
%}

%include "stdint.i"

// Only declare the missing holder functions, not all error functions
glue_schema_registry_error **new_glue_schema_registry_error_holder(void);
void delete_glue_schema_registry_error_holder(glue_schema_registry_error **p_err);

// Add accessor functions for safe error handling
char *glue_schema_registry_error_get_msgs(glue_schema_registry_error *error);
int glue_schema_registry_error_get_code(glue_schema_registry_error *error);
