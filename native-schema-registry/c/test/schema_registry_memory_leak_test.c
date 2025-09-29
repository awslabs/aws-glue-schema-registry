#include "glue_schema_registry_serializer.h"
#include "glue_schema_registry_deserializer.h"
#include "glue_schema_registry_schema.h"
#include "mutable_byte_array.h"
#include "read_only_byte_array.h"
#include <stdlib.h>
#include "cmocka.h"
#include "glue_schema_registry_test_helper.h"
#include "libnativeschemaregistry_mock.h"

#define TEST_SCHEMA_NAME "Employee.proto"
#define TEST_DATA_FORMAT "PROTOBUF"
#define TEST_SCHEMA_DEF "message Employee { string name = 1; int32 rank = 2;}"
#define TEST_TRANSPORT_NAME "test-transport"
#define NUM_SERDE_ITERATIONS 10000

static void test_serializer_lifecycle_memory_management(void **state) {
    // Test the complete serializer lifecycle: create, encode, dispose
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    
    // 1. CREATE: Initialize serializer
    glue_schema_registry_serializer *serializer = new_glue_schema_registry_serializer(NULL, "memory-test-agent", p_err);
    assert_non_null(serializer);
    assert_null(*p_err);
    
    // Create test data
    glue_schema_registry_schema *schema = new_glue_schema_registry_schema(
        TEST_SCHEMA_NAME, 
        TEST_SCHEMA_DEF, 
        TEST_DATA_FORMAT, 
        NULL, 
        p_err
    );
    assert_non_null(schema);
    assert_null(*p_err);
    
    read_only_byte_array *input_data = get_read_only_byte_array_fixture();
    assert_non_null(input_data);
    
    // 2. ENCODE: Use the serializer
    mutable_byte_array *encoded_data = glue_schema_registry_serializer_encode(
        serializer,
        input_data,
        TEST_TRANSPORT_NAME,
        schema,
        p_err
    );
    assert_non_null(encoded_data);
    assert_null(*p_err);
    
    // 3. DISPOSE: Clean up all resources
    delete_mutable_byte_array(encoded_data);
    delete_read_only_byte_array(input_data);
    delete_glue_schema_registry_schema(schema);
    delete_glue_schema_registry_serializer(serializer);
    delete_glue_schema_registry_error_holder(p_err);
    
    clear_mock_state();
}

static void test_deserializer_lifecycle_memory_management(void **state) {
    // Test the complete deserializer lifecycle: create, decode, dispose
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    
    // 1. CREATE: Initialize deserializer
    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL, "memory-test-agent", p_err);
    assert_non_null(deserializer);
    assert_null(*p_err);
    
    // Create test encoded data
    read_only_byte_array *encoded_data = get_read_only_byte_array_fixture();
    assert_non_null(encoded_data);
    
    // 2. DECODE: Use the deserializer
    mutable_byte_array *decoded_data = glue_schema_registry_deserializer_decode(
        deserializer,
        encoded_data,
        p_err
    );
    assert_non_null(decoded_data);
    assert_null(*p_err);
    
    // Also test schema decoding
    glue_schema_registry_schema *decoded_schema = glue_schema_registry_deserializer_decode_schema(
        deserializer,
        encoded_data,
        p_err
    );
    assert_non_null(decoded_schema);
    assert_null(*p_err);
    
    // 3. DISPOSE: Clean up all resources
    delete_mutable_byte_array(decoded_data);
    delete_glue_schema_registry_schema(decoded_schema);
    delete_read_only_byte_array(encoded_data);
    delete_glue_schema_registry_deserializer(deserializer);
    delete_glue_schema_registry_error_holder(p_err);
    
    clear_mock_state();
}

static void test_full_serialization_deserialization_cycle(void **state) {
    // Test complete round-trip: serialize then deserialize
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    
    // CREATE: Initialize both serializer and deserializer
    glue_schema_registry_serializer *serializer = new_glue_schema_registry_serializer(NULL, "cycle-test-agent", p_err);
    assert_non_null(serializer);
    assert_null(*p_err);
    
    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL, "cycle-test-agent", p_err);
    assert_non_null(deserializer);
    assert_null(*p_err);
    
    // Create test data and schema
    glue_schema_registry_schema *original_schema = new_glue_schema_registry_schema(
        TEST_SCHEMA_NAME, 
        TEST_SCHEMA_DEF, 
        TEST_DATA_FORMAT, 
        NULL, 
        p_err
    );
    assert_non_null(original_schema);
    
    read_only_byte_array *original_data = get_read_only_byte_array_fixture();
    assert_non_null(original_data);
    
    // ENCODE: Serialize the data
    mutable_byte_array *encoded_data = glue_schema_registry_serializer_encode(
        serializer,
        original_data,
        TEST_TRANSPORT_NAME,
        original_schema,
        p_err
    );
    assert_non_null(encoded_data);
    assert_null(*p_err);
    
    // Convert to read-only for deserialization
    read_only_byte_array *encoded_readonly = new_read_only_byte_array(
        encoded_data->data, 
        encoded_data->max_len, 
        p_err
    );
    assert_non_null(encoded_readonly);
    
    // DECODE: Deserialize the data back
    mutable_byte_array *decoded_data = glue_schema_registry_deserializer_decode(
        deserializer,
        encoded_readonly,
        p_err
    );
    assert_non_null(decoded_data);
    assert_null(*p_err);
    
    // DISPOSE: Clean up all resources in proper order
    delete_mutable_byte_array(decoded_data);
    delete_read_only_byte_array(encoded_readonly);
    delete_mutable_byte_array(encoded_data);
    delete_read_only_byte_array(original_data);
    delete_glue_schema_registry_schema(original_schema);
    delete_glue_schema_registry_deserializer(deserializer);
    delete_glue_schema_registry_serializer(serializer);
    delete_glue_schema_registry_error_holder(p_err);
    
    clear_mock_state();
}

static void test_multiple_operations_memory_management(void **state) {
    // Test multiple encode/decode operations with same instances
    set_mock_state(GRAAL_VM_INIT_SUCCESS);
    
    glue_schema_registry_error **p_err = new_glue_schema_registry_error_holder();
    
    // CREATE: Initialize serializer and deserializer once
    glue_schema_registry_serializer *serializer = new_glue_schema_registry_serializer(NULL, "multi-test-agent", p_err);
    assert_non_null(serializer);
    
    glue_schema_registry_deserializer *deserializer = new_glue_schema_registry_deserializer(NULL, "multi-test-agent", p_err);
    assert_non_null(deserializer);
    
    // Perform multiple operations to test for cumulative leaks
    for (int iteration = 0; iteration < NUM_SERDE_ITERATIONS; iteration++) {
        // Create fresh data for each iteration
        glue_schema_registry_schema *schema = new_glue_schema_registry_schema(
            TEST_SCHEMA_NAME, 
            TEST_SCHEMA_DEF, 
            TEST_DATA_FORMAT, 
            NULL, 
            p_err
        );
        assert_non_null(schema);
        
        read_only_byte_array *input_data = get_read_only_byte_array_fixture();
        assert_non_null(input_data);
        
        // ENCODE
        mutable_byte_array *encoded = glue_schema_registry_serializer_encode(
            serializer,
            input_data,
            TEST_TRANSPORT_NAME,
            schema,
            p_err
        );
        assert_non_null(encoded);
        
        // Convert for decoding
        read_only_byte_array *encoded_readonly = new_read_only_byte_array(
            encoded->data, 
            encoded->max_len, 
            p_err
        );
        assert_non_null(encoded_readonly);
        
        // DECODE
        mutable_byte_array *decoded = glue_schema_registry_deserializer_decode(
            deserializer,
            encoded_readonly,
            p_err
        );
        assert_non_null(decoded);
        
        // Clean up iteration resources
        delete_mutable_byte_array(decoded);
        delete_read_only_byte_array(encoded_readonly);
        delete_mutable_byte_array(encoded);
        delete_read_only_byte_array(input_data);
        delete_glue_schema_registry_schema(schema);
    }
    
    // DISPOSE: Clean up persistent resources
    delete_glue_schema_registry_deserializer(deserializer);
    delete_glue_schema_registry_serializer(serializer);
    delete_glue_schema_registry_error_holder(p_err);
    
    clear_mock_state();
}

int main(void) {
    const struct CMUnitTest tests[] = {
            cmocka_unit_test(test_serializer_lifecycle_memory_management),
            cmocka_unit_test(test_deserializer_lifecycle_memory_management),
            cmocka_unit_test(test_full_serialization_deserialization_cycle),
            cmocka_unit_test(test_multiple_operations_memory_management)
    };

    return cmocka_run_group_tests(tests, NULL, NULL);
}
