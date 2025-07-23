#include "../include/glue_schema_registry_config.h"
#include "../include/glue_schema_registry_error.h"
#include "../../target/libnativeschemaregistry.h"
#include <stdlib.h>

int glue_schema_registry_set_config_file(const char* config_file_path, glue_schema_registry_error **p_err) {
    if (config_file_path == NULL) {
        if (p_err != NULL) {
            *p_err = new_glue_schema_registry_error("Config file path cannot be NULL", ERR_CODE_NULL_PARAMETERS);
        }
        return -1;
    }

    graal_isolatethread_t *thread;
    int ret = graal_create_isolate(NULL, NULL, &thread);
    if (ret != 0) {
        if (p_err != NULL) {
            *p_err = new_glue_schema_registry_error("Failed to initialize GraalVM isolate for configuration", ERR_CODE_GRAALVM_INIT_EXCEPTION);
        }
        return ret;
    }
    
    // Call the Java configuration function with the thread context
    int result = ReadConfigurationFile(thread, (char*)config_file_path);
    
    // Clean up the isolate thread
    graal_tear_down_isolate(thread);
    
    if (result != 0 && p_err != NULL) {
        *p_err = new_glue_schema_registry_error("Failed to read configuration file", ERR_CODE_RUNTIME_ERROR);
    }
    
    return result;
}