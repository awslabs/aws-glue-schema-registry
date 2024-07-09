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
package com.amazonaws.services.schemaregistry.serializers;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.SchemaV2;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.Compatibility;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
public class GlueSchemaRegistrySerializerImplTest {
    private static final String TRANSPORT_NAME = "stream-foo";
    private static final String REGION = "us-west-2";

    private GlueSchemaRegistrySerializerImpl glueSchemaRegistrySerializer;

    @Mock
    private AwsCredentialsProvider credentialsProvider;

    @Mock
    private GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade;

    private final static byte[] ENCODED_DATA = new byte[] { 8, 9, 12, 83, 82 };
    private final static byte[] USER_DATA = new byte[] { 12, 83, 82 };
    private final static Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", "AVRO", "schemaFoo");
    private final static SchemaV2 SCHEMA_REGISTRY_SCHEMAV2 = new SchemaV2("{}", "AVRO", "schemaFoo", Compatibility.FORWARD);

    @BeforeEach
    void setUp() {
        glueSchemaRegistrySerializer = new GlueSchemaRegistrySerializerImpl(glueSchemaRegistrySerializationFacade);
    }

    @Test
    public void instantiate_WithNullConfiguration_CreatesInstance() {
        GlueSchemaRegistrySerializer glueSchemaRegistrySerializer =
            new GlueSchemaRegistrySerializerImpl(credentialsProvider, new GlueSchemaRegistryConfiguration(REGION));

        assertNotNull(glueSchemaRegistrySerializer);
    }

    @Test
    public void instantiate_WithConfiguration_CreatesInstance() {
        GlueSchemaRegistryConfiguration configuration = new GlueSchemaRegistryConfiguration(REGION);

        GlueSchemaRegistrySerializer glueSchemaRegistrySerializer =
            new GlueSchemaRegistrySerializerImpl(credentialsProvider, configuration);

        assertNotNull(glueSchemaRegistrySerializer);
    }

    @Test
    public void getSchema_WhenASchemaIsPassed_EncodesIntoSchemaRegistryMessage() {
        doReturn(ENCODED_DATA)
            .when(glueSchemaRegistrySerializationFacade).encode(TRANSPORT_NAME, SCHEMA_REGISTRY_SCHEMA, USER_DATA);

        byte [] actual = glueSchemaRegistrySerializer.encode(TRANSPORT_NAME, SCHEMA_REGISTRY_SCHEMA, USER_DATA);

        assertEquals(ENCODED_DATA, actual);
    }

    @Test
    public void getSchemaV2_WhenASchemaV2IsPassed_EncodesIntoSchemaRegistryMessage() {
        doReturn(ENCODED_DATA)
                .when(glueSchemaRegistrySerializationFacade).encodeV2(TRANSPORT_NAME, SCHEMA_REGISTRY_SCHEMAV2, USER_DATA);

        byte [] actual = glueSchemaRegistrySerializer.encodeV2(TRANSPORT_NAME, SCHEMA_REGISTRY_SCHEMAV2, USER_DATA);

        assertEquals(ENCODED_DATA, actual);
    }
}