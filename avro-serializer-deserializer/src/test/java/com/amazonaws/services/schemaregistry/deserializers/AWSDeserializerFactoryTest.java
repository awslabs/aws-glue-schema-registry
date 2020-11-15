/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.services.schemaregistry.deserializers;

import com.amazonaws.services.schemaregistry.common.AWSDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSAvroDeserializer;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for testing protocol specific instantiation factory.
 */
@ExtendWith(MockitoExtension.class)
public class AWSDeserializerFactoryTest {

    /**
     * Sets the configuration elements for testing.
     *
     * @return returns a configuration map
     */
    private Map<String, Object> getTestConfigMap() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        configMap.put(AWSSchemaRegistryConstants.COMPRESSION_TYPE, AWSSchemaRegistryConstants.COMPRESSION.NONE.name());
        configMap.put(AWSSchemaRegistryConstants.AWS_ENDPOINT,
                "https://test");
        configMap.put(AWSSchemaRegistryConstants.AWS_REGION, "US-West-1");
        return configMap;
    }

    /**
     * Test for Avro de-serializer instance creation with combinations of configurations.
     */
    @Test
    public void testGetInstance_Avro_createObject_succeeds() {
        Map<String, Object> configMap = getTestConfigMap();

        GlueSchemaRegistryConfiguration configs = new GlueSchemaRegistryConfiguration(configMap);
        AWSDeserializerFactory awsDeserializerFactory = new AWSDeserializerFactory();
        AWSDataFormatDeserializer deserializer = awsDeserializerFactory.getInstance(DataFormat.AVRO, configs);
        assertEquals(AWSAvroDeserializer.class, deserializer.getClass());
    }

    /**
     * Test for unsupported de-serializer instance creation with combinations of
     * configurations.
     */
    @Test
    public void testGetInstance_UnsupportedDataFormat_throwsException() {
        AWSDeserializerFactory awsDeserializerFactory = new AWSDeserializerFactory();
        assertThrows((UnsupportedOperationException.class), () -> awsDeserializerFactory.getInstance(DataFormat.UNKNOWN_TO_SDK_VERSION,
                new GlueSchemaRegistryConfiguration(getTestConfigMap())));
    }

    /**
     * Test for  de-serializer getInstance with null Data Format.
     */
    @Test
    public void testGetInstance_nullDataFormat_throwsException() {
        AWSDeserializerFactory awsDeserializerFactory = new AWSDeserializerFactory();
        assertThrows((IllegalArgumentException.class), () -> awsDeserializerFactory.getInstance(null,
                new GlueSchemaRegistryConfiguration(getTestConfigMap())));
    }

    /**
     * Test for  de-serializer getInstance with null Data Format.
     */
    @Test
    public void testGetInstance_nullConfigs_throwsException() {
        AWSDeserializerFactory awsDeserializerFactory = new AWSDeserializerFactory();
        assertThrows((IllegalArgumentException.class), () -> awsDeserializerFactory.getInstance(DataFormat.AVRO, null));
    }
}
