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

package com.amazonaws.services.schemaregistry.serializers.avro;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils;
import org.apache.avro.Schema;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GlueSchemaRegistryValidationUtil {
    /**
     * Helper function to load schema from file path
     *
     * @param fileName AVRO schema file location
     * @return
     * @throws IOException
     */
    protected Schema getSchema(String fileName) throws IOException {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(new File(fileName));
    }

    /**
     * Test helper method to mock build serializer with mocked client
     *
     * @param configs configs to initialize AWSKafkaAvroSerializer with
     * @param schemaDefinition schema definition will be used by mock client
     * @param mockClient fake schema registry client
     * @param testGenericSchemaVersionId test schema version id will be used by mock client
     * @return
     */
    protected AWSKafkaAvroSerializer initializeAWSKafkaAvroSerializer(Map<String, Object> configs,
                                                                      String schemaDefinition,
                                                                      AWSSchemaRegistryClient mockClient,
                                                                      UUID testGenericSchemaVersionId) {
        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(cred)
                        .schemaRegistryClient(mockClient)
                        .build();

        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq("User-Topic"),
                                                     eq(DataFormat.AVRO.name()), anyMap())).thenReturn(testGenericSchemaVersionId);
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = new AWSKafkaAvroSerializer(cred, null);
        awsKafkaAvroSerializer.configure(configs, true);

        awsKafkaAvroSerializer.setGlueSchemaRegistrySerializationFacade(glueSchemaRegistrySerializationFacade);
        return awsKafkaAvroSerializer;
    }

    /**
     * Test helper method to mock build serializer with mocked client
     *
     * @param configs configs to initialize AWSKafkaAvroSerializer with
     * @param mockClient fake schema registry client
     * @param schemaDefinitionToSchemaVersionIdMap map of test schema definitions to schema version ids for mock client
     * @return
     */
    protected AWSKafkaAvroSerializer initializeAWSKafkaAvroSerializer(Map<String, Object> configs, AWSSchemaRegistryClient mockClient,
                                                                      Map<String, UUID> schemaDefinitionToSchemaVersionIdMap) {
        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade
                .builder()
                .configs(configs)
                .credentialProvider(cred)
                .schemaRegistryClient(mockClient)
                .build();

        for (Map.Entry<String, UUID> entry : schemaDefinitionToSchemaVersionIdMap.entrySet()) {
            when(mockClient.getORRegisterSchemaVersionId(eq(entry.getKey()), eq("User-Topic"),
                                                         eq(DataFormat.AVRO.name()), anyMap())).thenReturn(entry.getValue());
        }
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = new AWSKafkaAvroSerializer(cred, null);
        awsKafkaAvroSerializer.configure(configs, true);

        awsKafkaAvroSerializer.setGlueSchemaRegistrySerializationFacade(glueSchemaRegistrySerializationFacade);
        return awsKafkaAvroSerializer;
    }

    /**
     * Test helper method to mock build serializer with pre existing schemaVersionId
     *
     * @param configs configs to initialize AWSKafkaAvroSerializer with
     * @param testGenericSchemaVersionId test schema version id will be used by mock client
     * @return
     */
    protected AWSKafkaAvroSerializer initializeAWSKafkaAvroSerializer(Map<String, Object> configs,
                                                                      UUID testGenericSchemaVersionId) {
        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);
        AWSSchemaRegistryClient mockClient = mock(AWSSchemaRegistryClient.class);
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(cred)
                        .schemaRegistryClient(mockClient)
                        .build();
        AWSKafkaAvroSerializer awsKafkaAvroSerializer = new AWSKafkaAvroSerializer(configs, testGenericSchemaVersionId);
        awsKafkaAvroSerializer.configure(configs, true);

        awsKafkaAvroSerializer.setGlueSchemaRegistrySerializationFacade(glueSchemaRegistrySerializationFacade);
        return awsKafkaAvroSerializer;
    }

    /**
     * Test helper method to mock build serializer with mocked client
     *
     * @param configs configs to initialize GlueSchemaRegistryKafkaSerializer with
     * @param schemaDefinition schema definition will be used by mock client
     * @param mockClient fake schema registry client
     * @param testGenericSchemaVersionId test schema version id will be used by mock client
     * @return
     */
    protected GlueSchemaRegistryKafkaSerializer initializeGSRKafkaSerializer(Map<String, Object> configs,
                                                                             String schemaDefinition,
                                                                             AWSSchemaRegistryClient mockClient,
                                                                             UUID testGenericSchemaVersionId) {
        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(cred)
                        .schemaRegistryClient(mockClient)
                        .build();

        when(mockClient.getORRegisterSchemaVersionId(eq(schemaDefinition), eq("User-Topic"),
                                                     eq(GlueSchemaRegistryUtils.getInstance().getDataFormat(configs)),
                                                     anyMap())).thenReturn(testGenericSchemaVersionId);
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                new GlueSchemaRegistryKafkaSerializer(cred, null);
        glueSchemaRegistryKafkaSerializer.configure(configs, true);

        glueSchemaRegistryKafkaSerializer.setGlueSchemaRegistrySerializationFacade(glueSchemaRegistrySerializationFacade);
        return glueSchemaRegistryKafkaSerializer;
    }

    /**
     * Test helper method to mock build serializer with mocked client
     *
     * @param configs configs to initialize GlueSchemaRegistryKafkaSerializer with
     * @param mockClient fake schema registry client
     * @param schemaDefinitionToSchemaVersionIdMap map of test schema definitions to schema version ids for mock client
     * @return
     */
    protected GlueSchemaRegistryKafkaSerializer initializeGSRKafkaSerializer(Map<String, Object> configs,
                                                                             AWSSchemaRegistryClient mockClient,
                                                                             Map<String, UUID> schemaDefinitionToSchemaVersionIdMap) {
        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);

        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(cred)
                        .schemaRegistryClient(mockClient)
                        .build();

        for (Map.Entry<String, UUID> entry : schemaDefinitionToSchemaVersionIdMap.entrySet()) {
            when(mockClient.getORRegisterSchemaVersionId(eq(entry.getKey()), eq("User-Topic"),
                                                         eq(GlueSchemaRegistryUtils.getInstance().getDataFormat(configs)),
                                                         anyMap())).thenReturn(entry.getValue());
        }
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer =
                new GlueSchemaRegistryKafkaSerializer(cred, null);
        glueSchemaRegistryKafkaSerializer.configure(configs, true);

        glueSchemaRegistryKafkaSerializer.setGlueSchemaRegistrySerializationFacade(glueSchemaRegistrySerializationFacade);
        return glueSchemaRegistryKafkaSerializer;
    }

    /**
     * Test helper method to mock build serializer with pre existing schemaVersionId
     *
     * @param configs configs to initialize GlueSchemaRegistryKafkaSerializer with
     * @param testGenericSchemaVersionId test schema version id will be used by mock client
     * @return
     */
    protected GlueSchemaRegistryKafkaSerializer initializeGSRKafkaSerializer(Map<String, Object> configs,
                                                                             UUID testGenericSchemaVersionId) {
        AwsCredentialsProvider cred = mock(AwsCredentialsProvider.class);
        AWSSchemaRegistryClient mockClient = mock(AWSSchemaRegistryClient.class);
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade =
                GlueSchemaRegistrySerializationFacade.builder()
                        .configs(configs)
                        .credentialProvider(cred)
                        .schemaRegistryClient(mockClient)
                        .build();
        GlueSchemaRegistryKafkaSerializer glueSchemaRegistryKafkaSerializer = new GlueSchemaRegistryKafkaSerializer(configs, testGenericSchemaVersionId);
        glueSchemaRegistryKafkaSerializer.configure(configs, true);

        glueSchemaRegistryKafkaSerializer.setGlueSchemaRegistrySerializationFacade(glueSchemaRegistrySerializationFacade);
        return glueSchemaRegistryKafkaSerializer;
    }

    /**
     * Helper function to test serialized data's bytes and schemaVersionId value
     *
     * @param serializedData serialized byte array
     * @param testGenericSchemaVersionId expected schemaVersionId value
     */
    protected void testForSerializedData(byte[] serializedData, UUID testGenericSchemaVersionId, AWSSchemaRegistryConstants.COMPRESSION compressionType) {
        testForSerializedData(serializedData, testGenericSchemaVersionId, compressionType, null);
    }

    protected void testForSerializedData(byte[] serializedData, UUID testGenericSchemaVersionId, AWSSchemaRegistryConstants.COMPRESSION compressionType, byte[] expectedPayload) {
        assertNotNull(serializedData);

        ByteBuffer buffer = getByteBuffer(serializedData);

        byte headerVersionByte = getByte(buffer);
        byte compressionByte = getByte(buffer);
        UUID schemaVersionId = getSchemaVersionId(buffer);

        assertEquals((byte) 3, headerVersionByte);
        assertEquals(testGenericSchemaVersionId, schemaVersionId);

        if (compressionType.name().equals(AWSSchemaRegistryConstants.COMPRESSION.NONE.name())) {
            assertEquals((byte) 0, compressionByte);
        } else {
            assertEquals((byte) 5, compressionByte);
        }

        if (expectedPayload != null) {
            byte[] actualPayload = new byte[buffer.remaining()];
            buffer.get(actualPayload);
            assertArrayEquals(expectedPayload, actualPayload);
        }
    }

    private ByteBuffer getByteBuffer(byte[] bytes) {
        return ByteBuffer.wrap(bytes);
    }

    private Byte getByte(ByteBuffer buffer) {
        return buffer.get();
    }

    private UUID getSchemaVersionId(ByteBuffer buffer) {
        long mostSigBits = buffer.getLong();
        long leastSigBits = buffer.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }
}
