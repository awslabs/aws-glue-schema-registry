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

package com.amazonaws.services.schemaregistry.common;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest;
import software.amazon.awssdk.services.glue.model.CreateSchemaResponse;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.GetTagsRequest;
import software.amazon.awssdk.services.glue.model.GetTagsResponse;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataResponse;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.SchemaId;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AWSSchemaRegistryClientTest {
    @Mock
    private GlueClient mockGlueClient;
    private final Map<String, Object> configs = new HashMap<>();
    private AWSSchemaRegistryClient awsSchemaRegistryClient;
    private GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;
    private static String userSchemaDefinition;
    private static GenericRecord genericUserAvroRecord;
    private Schema schema = null;
    private Map<String, String> testTags;

    private static final UUID SCHEMA_ID_FOR_TESTING = UUID.fromString("b7b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    public static final String AVRO_USER_SCHEMA_FILE = "src/test/java/resources/avro/user.avsc";

    @BeforeEach
    public void setup() {
        awsSchemaRegistryClient = new AWSSchemaRegistryClient(mockGlueClient);

        Schema.Parser parser = new Schema.Parser();
        try {
            schema = parser.parse(new File(AVRO_USER_SCHEMA_FILE));
        } catch (IOException e) {
            fail("Catch IOException: ", e);
        }

        genericUserAvroRecord = new GenericData.Record(schema);
        genericUserAvroRecord.put("name", "sansa");
        genericUserAvroRecord.put("favorite_number", 99);
        genericUserAvroRecord.put("favorite_color", "red");
        testTags = new HashMap<>();
        testTags.put("testKey", "testValue");

        userSchemaDefinition = AVROUtils.getInstance().getSchemaDefinition(genericUserAvroRecord);

        configs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
        configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "User-Topic");
        configs.put(AWSSchemaRegistryConstants.TAGS, testTags);
        glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
    }

    @Test
    public void testAWSSchemaRegistryClient_putSchemaVersionMetadata_succeeds() {
        Map<String, String> metadata = getMetadata();

        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            MetadataKeyValuePair metadataKeyValuePair = createMetadataKeyValuePair(entry);
            PutSchemaVersionMetadataRequest putSchemaVersionMetadataRequest =
                    createPutSchemaVersionMetadataRequest(SCHEMA_ID_FOR_TESTING, metadataKeyValuePair);
            PutSchemaVersionMetadataResponse putSchemaVersionMetadataResponse =
                    createPutSchemaVersionMetadataResponse(SCHEMA_ID_FOR_TESTING, metadataKeyValuePair);
            when(mockGlueClient.putSchemaVersionMetadata(putSchemaVersionMetadataRequest))
                    .thenReturn(putSchemaVersionMetadataResponse);
        }

        awsSchemaRegistryClient.putSchemaVersionMetadata(SCHEMA_ID_FOR_TESTING, metadata);
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            MetadataKeyValuePair metadataKeyValuePair = createMetadataKeyValuePair(entry);
            PutSchemaVersionMetadataRequest putSchemaVersionMetadataRequest =
                    createPutSchemaVersionMetadataRequest(SCHEMA_ID_FOR_TESTING, metadataKeyValuePair);
            verify(mockGlueClient, times(1)).putSchemaVersionMetadata(putSchemaVersionMetadataRequest);
        }
    }

    @Test
    public void testConstructor_nullSerdeConfigs_throwsException() {
        AwsCredentialsProvider mockAwsCredentialsProvider = mock(AwsCredentialsProvider.class);
        Assertions.assertThrows(IllegalArgumentException.class , () -> new AWSSchemaRegistryClient(mockAwsCredentialsProvider,null ));
    }

    @Test
    public void testConstructor_nullGlueClient_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class , () -> new AWSSchemaRegistryClient((GlueClient) null));
    }

    @Test
    public void testConstructor_withMalformedUri_throwsException() {
        glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        String invalidURL = "://abc:com";
        glueSchemaRegistryConfiguration.setEndPoint(invalidURL);
        AwsCredentialsProvider mockAwsCredentialsProvider = mock(AwsCredentialsProvider.class);
        AWSSchemaRegistryException awsSchemaRegistryException = Assertions.assertThrows(AWSSchemaRegistryException.class ,
                () -> new AWSSchemaRegistryClient(mockAwsCredentialsProvider, glueSchemaRegistryConfiguration));
        assertEquals(URISyntaxException.class, awsSchemaRegistryException.getCause().getClass());

        String expectedMessage = String.format("Malformed uri, please pass the valid uri for creating the client",
                glueSchemaRegistryConfiguration.getEndPoint());
        assertEquals(expectedMessage, awsSchemaRegistryException.getMessage());
    }

    /**
     * Tests positive case for querySchemaVersionMetadata by building request and response
     */
    @Test
    public void testQuerySchemaVersionMetadata_setSchemaVersionId_returnsResponseWithSchemaVersionId() {
        QuerySchemaVersionMetadataResponse querySchemaVersionMetadataResponse = QuerySchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build();

        when(mockGlueClient.querySchemaVersionMetadata(QuerySchemaVersionMetadataRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build())).thenReturn(querySchemaVersionMetadataResponse);

        assertEquals(SCHEMA_ID_FOR_TESTING.toString(), awsSchemaRegistryClient.querySchemaVersionMetadata(SCHEMA_ID_FOR_TESTING).schemaVersionId());
    }

    /**
     * Tests negative case for querySchemaVersionMetadata by checking the AWSSchemaRegistryException exception
     */
    @Test
    public void testQuerySchemaVersionMetadata_clientThrowsException_throwsAWSSchemaRegistryException() {
        when(mockGlueClient.querySchemaVersionMetadata(QuerySchemaVersionMetadataRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build())).thenThrow(new NullPointerException());

        try {
            awsSchemaRegistryClient.querySchemaVersionMetadata(SCHEMA_ID_FOR_TESTING);
        } catch (Exception e) {
            assertEquals(AWSSchemaRegistryException.class , e.getClass());
            String errorMessage = String.format("Query schema version metadata :: Call failed when query metadata for schema version id = %s",
                    SCHEMA_ID_FOR_TESTING.toString());
            assertEquals(errorMessage, e.getMessage());
        }
     }

    /**
     * Tests buildGetSchemaByDefinitionRequest by verifying schema name and definition
     */
    @Test
    public void testBuildGetSchemaByDefinitionRequest_validConfigs_buildsResponseSuccessfully() {
        glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        AwsCredentialsProvider mockAwsCredentialsProvider = mock(AwsCredentialsProvider.class);
        awsSchemaRegistryClient =  new AWSSchemaRegistryClient(mockAwsCredentialsProvider,
            glueSchemaRegistryConfiguration);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString());

        assertEquals(configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString(), getSchemaByDefinitionRequest.schemaId().schemaName());
        assertEquals(userSchemaDefinition, getSchemaByDefinitionRequest.schemaDefinition());
    }

    @Test
    public void testGetSchemaVersionIdByDefinition_nullSchemaVersionId_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class , () ->  awsSchemaRegistryClient
                .getSchemaVersionIdByDefinition(null, "test-schema-name", DataFormat.AVRO.name()));
    }

    @Test
    public void testGetSchemaVersionIdByDefinition_nullSchemaName_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class , () ->  awsSchemaRegistryClient
                .getSchemaVersionIdByDefinition(userSchemaDefinition, null, DataFormat.AVRO.name()));
    }

    @Test
    public void testGetSchemaVersionIdByDefinition_nullDataFormat_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class , () ->  awsSchemaRegistryClient
                .getSchemaVersionIdByDefinition(userSchemaDefinition, "test-schema-name", null));
    }

    @Test
    public void testGetSchemaVersionIdByDefinition_allParamsNonNull_schemaVersionIdMatches() throws NoSuchFieldException, IllegalAccessException {
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
            glueSchemaRegistryConfiguration);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
                .buildGetSchemaByDefinitionRequest(userSchemaDefinition, String.valueOf(configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME)));
        GetSchemaByDefinitionResponse getSchemaByDefinitionResponse = GetSchemaByDefinitionResponse.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build();

        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest)).thenReturn(getSchemaByDefinitionResponse);

        assertEquals(SCHEMA_ID_FOR_TESTING, awsSchemaRegistryClient.getSchemaVersionIdByDefinition(userSchemaDefinition,
                String.valueOf(configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME)), DataFormat.AVRO.name()));
    }

    @Test
    public void testGetSchemaVersionIdByDefinition_clientExceptionResponse_throwsAWSSchemaRegistryException() throws NoSuchFieldException, IllegalAccessException {
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
            glueSchemaRegistryConfiguration);
        mockGlueClient = null;

        AWSSchemaRegistryException awsSchemaRegistryException = assertThrows(AWSSchemaRegistryException.class, () ->
                awsSchemaRegistryClient.getSchemaVersionIdByDefinition(userSchemaDefinition,
                configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString(), DataFormat.AVRO.name()));

        String expectedExceptionMessage = String.format("Failed to get schemaVersionId by schema definition for schema name = %s ",
                configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString());;
        assertEquals(expectedExceptionMessage, awsSchemaRegistryException.getMessage());
    }

    @Test
    public void testGetSchemaVersionResponse_nullSchemaVersionId_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class , () ->  awsSchemaRegistryClient
                .getSchemaVersionResponse(null));
    }

    @Test
    public void testGetSchemaVersionResponse_setSchemaVersionId_returnsResponseSchemaVersionId() {
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder().schemaVersionId(SCHEMA_ID_FOR_TESTING.toString()).build();
        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder().schemaVersionId(SCHEMA_ID_FOR_TESTING.toString()).build();
        when(mockGlueClient.getSchemaVersion(getSchemaVersionRequest)).thenReturn(getSchemaVersionResponse);

        assertEquals(SCHEMA_ID_FOR_TESTING.toString(), awsSchemaRegistryClient.getSchemaVersionResponse(SCHEMA_ID_FOR_TESTING.toString()).schemaVersionId());
    }


    private Map<String, String> getConfigsWithAutoRegistrationSetting(boolean autoRegistrationSetting) {
        Map<String, String> localConfigs = new HashMap<>();
        localConfigs.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        localConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        localConfigs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "User-Topic");
        localConfigs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "User-Topic");
        localConfigs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING,
                         String.valueOf(autoRegistrationSetting));
        return localConfigs;
    }

    @Test
    public void testGetSchemaVersionResponse_clientExceptionResponse_returnsAWSSchemaRegistryException() {
        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder().schemaVersionId(SCHEMA_ID_FOR_TESTING.toString()).build();
        when(mockGlueClient.getSchemaVersion(getSchemaVersionRequest)).thenThrow(EntityNotFoundException.class);

        try {
            awsSchemaRegistryClient.getSchemaVersionResponse(SCHEMA_ID_FOR_TESTING.toString());
        } catch (Exception e) {
            assertEquals(EntityNotFoundException.class, e.getCause().getClass());
            assertEquals(AWSSchemaRegistryException.class, e.getClass());
            String expectedErrorMessage = "Failed to get schema version Id = " + SCHEMA_ID_FOR_TESTING;
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }

    @Test
    public void testCreateSchema_schemaNameWithDataFormat_returnsResponseSuccessfully() throws NoSuchFieldException, IllegalAccessException {
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString();
        String dataFormatName = DataFormat.AVRO.name();
        String schemaVersionId = UUID.randomUUID().toString();

        CreateSchemaResponse createSchemaResponse = CreateSchemaResponse.builder()
                .schemaName(schemaName)
                .dataFormat(dataFormatName)
                .schemaVersionId(schemaVersionId)
                .build();
        CreateSchemaRequest createSchemaRequest = CreateSchemaRequest.builder()
                .dataFormat(DataFormat.AVRO)
                .description(glueSchemaRegistryConfiguration.getDescription())
                .schemaName(schemaName)
                .schemaDefinition(userSchemaDefinition)
                .compatibility(glueSchemaRegistryConfiguration.getCompatibilitySetting())
                .tags(glueSchemaRegistryConfiguration.getTags())
                .registryId(RegistryId.builder().registryName(glueSchemaRegistryConfiguration.getRegistryName()).build())
                .build();

        when(mockGlueClient.createSchema(createSchemaRequest)).thenReturn(createSchemaResponse);
        assertEquals(UUID.fromString(schemaVersionId), awsSchemaRegistryClient
                .createSchema(schemaName, dataFormatName, userSchemaDefinition, getMetadata()));
    }

    @Test
    public void testCreateSchema_clientExceptionResponse_returnsAWSSchemaRegistryException() throws NoSuchFieldException, IllegalAccessException {
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
            glueSchemaRegistryConfiguration);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString();
        String dataFormatName = DataFormat.AVRO.name();
        CreateSchemaRequest createSchemaRequest = CreateSchemaRequest.builder()
                .dataFormat(DataFormat.AVRO)
                .description(glueSchemaRegistryConfiguration.getDescription())
                .schemaName(schemaName)
                .schemaDefinition(userSchemaDefinition)
                .compatibility(glueSchemaRegistryConfiguration.getCompatibilitySetting())
                .tags(glueSchemaRegistryConfiguration.getTags())
                .registryId(RegistryId.builder().registryName(glueSchemaRegistryConfiguration.getRegistryName()).build())
                .build();

        when(mockGlueClient.createSchema(createSchemaRequest)).thenThrow(EntityNotFoundException.class);

        try {
            awsSchemaRegistryClient.createSchema(schemaName, dataFormatName, userSchemaDefinition, getMetadata());
        } catch (Exception e) {
            assertEquals(EntityNotFoundException.class, e.getCause().getClass());
            assertEquals(AWSSchemaRegistryException.class, e.getClass());
            String expectedErrorMessage = "Create schema :: Call failed when creating the schema with the schema registry for schema name = " + schemaName;
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }

    @Test
    public void testRegisterSchemaVersion_validParameters_returnsResponseWithSchemaVersionId() throws NoSuchFieldException, IllegalAccessException {
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
            glueSchemaRegistryConfiguration);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString();
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME).toString();
        String dataFormatName = DataFormat.AVRO.name();
        Long schemaVersionNumber = 1L;
        SchemaId requestSchemaId = SchemaId.builder().schemaName(schemaName).registryName(registryName).build();

        RegisterSchemaVersionRequest registerSchemaVersionRequest = RegisterSchemaVersionRequest.builder()
                .schemaDefinition(userSchemaDefinition)
                .schemaId(requestSchemaId)
                .build();
        RegisterSchemaVersionResponse registerSchemaVersionResponse = RegisterSchemaVersionResponse.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .versionNumber(schemaVersionNumber)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build();
        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build();

        when(mockGlueClient.registerSchemaVersion(registerSchemaVersionRequest)).thenReturn(registerSchemaVersionResponse);

        assertEquals(SCHEMA_ID_FOR_TESTING.toString(), awsSchemaRegistryClient.registerSchemaVersion(userSchemaDefinition, schemaName, dataFormatName).schemaVersionId());
        verify(mockGlueClient, times(0)).getSchemaVersion(getSchemaVersionRequest);
    }

    @ParameterizedTest
    @EnumSource(value = AWSSchemaRegistryConstants.SchemaVersionStatus.class, mode = EnumSource.Mode.EXCLUDE, names = { "AVAILABLE" })
    public void testRegisterSchemaVersion_statusIsNotAvailable_throwsException(AWSSchemaRegistryConstants.SchemaVersionStatus schemaVersionStatus) throws NoSuchFieldException,
            IllegalAccessException {
        Map<String, String> configs = getConfigsWithAutoRegistrationSetting(false);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String dataFormatName = DataFormat.AVRO.name();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
                                                                                  glueSchemaRegistryConfiguration);

        RegisterSchemaVersionResponse registerSchemaVersionResponse = RegisterSchemaVersionResponse.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(schemaVersionStatus.toString())
                .build();

        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build();
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .schemaDefinition(userSchemaDefinition)
                .status(schemaVersionStatus.toString())
                .build();

        when(mockGlueClient.registerSchemaVersion(any(RegisterSchemaVersionRequest.class)))
                .thenReturn(registerSchemaVersionResponse);
        when(mockGlueClient.getSchemaVersion(getSchemaVersionRequest))
                .thenReturn(getSchemaVersionResponse);

        Exception exception = assertThrows(AWSSchemaRegistryException.class, ()
                -> awsSchemaRegistryClient.registerSchemaVersion(userSchemaDefinition, schemaName,
                                                                 dataFormatName));
        assertEquals(exception.getMessage(), String.format("Register schema :: Call failed when " + "registering the "
                                                           + "schema with the schema registry for schema name = %s",
                                                           schemaName));
        assertEquals(exception.getCause()
                             .getMessage(), String.format("Exception occurred, while performing schema evolution "
                                                          + "check for schemaVersionId = %s",
                                                          getSchemaVersionRequest.schemaVersionId()));

        if(AWSSchemaRegistryConstants.SchemaVersionStatus.PENDING.equals(schemaVersionStatus)) {
            verify(mockGlueClient, times(10)).getSchemaVersion(getSchemaVersionRequest);
        }

        if (AWSSchemaRegistryConstants.SchemaVersionStatus.DELETING.equals(schemaVersionStatus)
            || AWSSchemaRegistryConstants.SchemaVersionStatus.FAILURE.equals(schemaVersionStatus)) {
            verify(mockGlueClient, times(1)).getSchemaVersion(getSchemaVersionRequest);
        }

    }

    @Test
    public void testRegisterSchemaVersion_statusEvolvesToAvailable_succeeds() throws NoSuchFieldException,
            IllegalAccessException {
        Map<String, String> configs = getConfigsWithAutoRegistrationSetting(false);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME).toString();
        String dataFormatName = DataFormat.AVRO.name();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistryClient =
                configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
                                                                glueSchemaRegistryConfiguration);

        SchemaId requestSchemaId = SchemaId.builder().schemaName(schemaName).registryName(registryName).build();

        RegisterSchemaVersionRequest registerSchemaVersionRequest = RegisterSchemaVersionRequest.builder()
                .schemaDefinition(userSchemaDefinition)
                .schemaId(requestSchemaId)
                .build();

        RegisterSchemaVersionResponse registerSchemaVersionResponse = RegisterSchemaVersionResponse.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.PENDING.toString())
                .build();

        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build();

        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .schemaDefinition(userSchemaDefinition)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build();

        when(mockGlueClient.registerSchemaVersion(registerSchemaVersionRequest)).thenReturn(registerSchemaVersionResponse);
        when(mockGlueClient.getSchemaVersion(getSchemaVersionRequest)).thenReturn(getSchemaVersionResponse);

        assertEquals(SCHEMA_ID_FOR_TESTING.toString(), awsSchemaRegistryClient.registerSchemaVersion(userSchemaDefinition, schemaName, dataFormatName).schemaVersionId());
        verify(mockGlueClient, times(1)).getSchemaVersion(getSchemaVersionRequest);

    }

    @Test
    public void testRegisterSchemaVersion_clientThrowsException_throwsAWSSchemaRegistryException() throws NoSuchFieldException, IllegalAccessException {
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
            glueSchemaRegistryConfiguration);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME).toString();
        String dataFormatName = DataFormat.AVRO.name();
        mockGlueClient = null;
        assertThrows(AWSSchemaRegistryException.class, () -> awsSchemaRegistryClient.registerSchemaVersion(userSchemaDefinition, schemaName, dataFormatName));
    }

    @Test
    public void  testGetSchemaIdRequestObject_nullSchemaName_throwsException() throws NoSuchMethodException {
        Method getSchemaIdRequestObjectMethod = AWSSchemaRegistryClient.class.getDeclaredMethod("getSchemaIdRequestObject", String.class, String.class);
        getSchemaIdRequestObjectMethod.setAccessible(true);

        try {
            getSchemaIdRequestObjectMethod.invoke(awsSchemaRegistryClient, null, "test-registry-name");
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        }
    }

    @Test
    public void  testGetSchemaIdRequestObject_nullRegistryName_throwsException() throws NoSuchMethodException {
        Method getSchemaIdRequestObjectMethod = AWSSchemaRegistryClient.class.getDeclaredMethod("getSchemaIdRequestObject", String.class, String.class);
        getSchemaIdRequestObjectMethod.setAccessible(true);

        try {
            getSchemaIdRequestObjectMethod.invoke(awsSchemaRegistryClient, "test-schema-name", null);
        } catch (Exception e) {
            assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        }
    }

    @Test
    public void  testValidateSchemaVersionResponse_nullSchemaName_throwsException() throws NoSuchMethodException {
        Method validateSchemaVersionResponseMethod = AWSSchemaRegistryClient.class.getDeclaredMethod("validateSchemaVersionResponse",
                GetSchemaVersionResponse.class, String.class);
        validateSchemaVersionResponseMethod.setAccessible(true);
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder().build();

        try {
            validateSchemaVersionResponseMethod.invoke(awsSchemaRegistryClient, getSchemaVersionResponse, null);
        } catch (Exception e) {
            String exceptionMessage = String.format("Schema definition is not present for the schema id = %s", null);
            assertEquals(AWSSchemaRegistryException.class, e.getCause().getClass());
            assertEquals(exceptionMessage, e.getCause().getMessage());
        }
    }

    @Test
    public void  testValidateSchemaVersionResponse_nullGetSchemaVersionResponse_throwsException() throws NoSuchMethodException {
        Method validateSchemaVersionResponseMethod = AWSSchemaRegistryClient.class.getDeclaredMethod("validateSchemaVersionResponse",
                GetSchemaVersionResponse.class, String.class);
        validateSchemaVersionResponseMethod.setAccessible(true);

        try {
            validateSchemaVersionResponseMethod.invoke(awsSchemaRegistryClient, null, "test-schema-name");
        } catch (Exception e) {
            String exceptionMessage = String.format("Schema definition is not present for the schema id = test-schema-name");
            assertEquals(AWSSchemaRegistryException.class, e.getCause().getClass());
            assertEquals(exceptionMessage, e.getCause().getMessage());
        }
    }

    @Test
    public void  testReturnSchemaVersionIdIfAvailable_nullSchemaVersionId_throwsException() throws NoSuchMethodException {
        Method returnSchemaVersionIdIfAvailableMethod = AWSSchemaRegistryClient.class.getDeclaredMethod("returnSchemaVersionIdIfAvailable",
                GetSchemaByDefinitionResponse.class);
        returnSchemaVersionIdIfAvailableMethod.setAccessible(true);
        GetSchemaByDefinitionResponse getSchemaByDefinitionResponse = GetSchemaByDefinitionResponse.builder()
                .schemaVersionId(null)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build();

        try {
            returnSchemaVersionIdIfAvailableMethod.invoke(awsSchemaRegistryClient,  getSchemaByDefinitionResponse);
        } catch (Exception e) {
            String exceptionMessage = String.format("Schema Found but status is %s", getSchemaByDefinitionResponse.statusAsString());
            assertEquals(AWSSchemaRegistryException.class, e.getCause().getClass());
            assertEquals(exceptionMessage, e.getCause().getMessage());
        }
    }

    @Test
    public void  testReturnSchemaVersionIdIfAvailable_nullStatusString_throwsException() throws NoSuchMethodException {
        Method returnSchemaVersionIdIfAvailableMethod = AWSSchemaRegistryClient.class.getDeclaredMethod("returnSchemaVersionIdIfAvailable",
                GetSchemaByDefinitionResponse.class);
        returnSchemaVersionIdIfAvailableMethod.setAccessible(true);
        GetSchemaByDefinitionResponse getSchemaByDefinitionResponse = GetSchemaByDefinitionResponse.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status("invalidStatus")
                .build();

        try {
            returnSchemaVersionIdIfAvailableMethod.invoke(awsSchemaRegistryClient,  getSchemaByDefinitionResponse);
        } catch (Exception e) {
            String exceptionMessage = String.format("Schema Found but status is %s", getSchemaByDefinitionResponse.statusAsString());
            assertEquals(AWSSchemaRegistryException.class, e.getCause().getClass());
            assertEquals(exceptionMessage, e.getCause().getMessage());
        }
    }

    @Test
    public void  testWaitForSchemaEvolutionCheckToComplete_resultsAvailableResponse_returnsResponseWithSchemaId() throws NoSuchMethodException {
        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build();
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .schemaDefinition(userSchemaDefinition)
                .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
                .build();

        when(mockGlueClient.getSchemaVersion(getSchemaVersionRequest)).thenReturn(getSchemaVersionResponse);
        Method waitForSchemaEvolutionCheckToCompleteMethod = AWSSchemaRegistryClient.class.getDeclaredMethod(
                "waitForSchemaEvolutionCheckToComplete", GetSchemaVersionRequest.class);
        waitForSchemaEvolutionCheckToCompleteMethod.setAccessible(true);
        GetSchemaVersionResponse resultResponse = (GetSchemaVersionResponse) assertDoesNotThrow(() ->
                waitForSchemaEvolutionCheckToCompleteMethod.invoke(awsSchemaRegistryClient, getSchemaVersionRequest));

        assertEquals(SCHEMA_ID_FOR_TESTING.toString(), resultResponse.schemaVersionId());
    }

    @Test
    public void  testWaitForSchemaEvolutionCheckToComplete_clientThrowsException_throwsException() throws NoSuchMethodException {
        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .build();

        mockGlueClient = null;
        Method waitForSchemaEvolutionCheckToCompleteMethod = AWSSchemaRegistryClient.class.getDeclaredMethod(
                "waitForSchemaEvolutionCheckToComplete", GetSchemaVersionRequest.class);
        waitForSchemaEvolutionCheckToCompleteMethod.setAccessible(true);

        try {
            waitForSchemaEvolutionCheckToCompleteMethod.invoke(awsSchemaRegistryClient, getSchemaVersionRequest);
        } catch (Exception e) {
            assertEquals(AWSSchemaRegistryException.class, e.getCause().getClass());
            String expectedExceptionMessage = String.format(
                    "Exception occurred, while performing schema evolution check for schemaVersionId = %s",
                    getSchemaVersionRequest.schemaVersionId());
            assertEquals(expectedExceptionMessage, e.getCause().getMessage());
        }
    }

    @Test
    public void testQuerySchemaTags_validGetTagsRequest_returnsValidResponse() throws NoSuchFieldException, IllegalAccessException {
        String testSchemaName = "test-schema";
        String testSchemaDefinition = "test-schema-definition";
        String testSchemaARN = "test-schema-arn";
        Map<String, String> expectedTags = glueSchemaRegistryConfiguration.getTags();
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
            glueSchemaRegistryConfiguration);

        GetSchemaByDefinitionResponse getSchemaByDefinitionResponse =
                GetSchemaByDefinitionResponse.builder().schemaArn(testSchemaARN).build();
        System.out.println("getSchemaByDefinitionResponse:" + getSchemaByDefinitionResponse);

        when(mockGlueClient.getSchemaByDefinition(awsSchemaRegistryClient.buildGetSchemaByDefinitionRequest(testSchemaDefinition, testSchemaName)))
                .thenReturn(getSchemaByDefinitionResponse);

        GetTagsRequest getTagsRequest = GetTagsRequest.builder().resourceArn(testSchemaARN).build();
        GetTagsResponse getTagsResponse = GetTagsResponse.builder().tags(expectedTags).build();
        when(mockGlueClient.getTags(getTagsRequest)).thenReturn(getTagsResponse);

        Map<String, String> responseTags = assertDoesNotThrow(() -> awsSchemaRegistryClient.querySchemaTags(testSchemaDefinition, testSchemaName).tags());

        assertNotNull(responseTags);
        assertEquals(expectedTags.size(), responseTags.size());
        assertTrue(expectedTags.containsKey("testKey"));
        assertEquals(expectedTags.get("testKey"), responseTags.get("testKey"));
    }

    @Test
    public void testQuerySchemaTags_clientThrowsException_throwsException() throws NoSuchFieldException, IllegalAccessException {
        String testSchemaName = "test-schema";
        String testSchemaDefinition = "test-schema-definition";
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
            glueSchemaRegistryConfiguration);
        mockGlueClient = null;

        AWSSchemaRegistryException awsSchemaRegistryException = assertThrows(AWSSchemaRegistryException.class, ()
                -> awsSchemaRegistryClient.querySchemaTags(testSchemaDefinition, testSchemaName));

        String expectedMsg = String.format("Query schema tags:: Call failed while querying tags for schema = %s", testSchemaName);
        assertEquals(expectedMsg, awsSchemaRegistryException.getMessage());
    }

    private Map<String, String> getMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("event-source-1", "topic1");
        metadata.put("event-source-2", "topic2");
        metadata.put("event-source-3", "topic3");
        metadata.put("event-source-4", "topic4");
        metadata.put("event-source-5", "topic5");

        return metadata;
    }

    private MetadataKeyValuePair createMetadataKeyValuePair(Map.Entry<String, String> metadataEntry) {
        return MetadataKeyValuePair
                .builder()
                .metadataKey(metadataEntry.getKey())
                .metadataValue(metadataEntry.getValue())
                .build();
    }

    private PutSchemaVersionMetadataRequest createPutSchemaVersionMetadataRequest(
            UUID schemaVersionId, MetadataKeyValuePair metadataKeyValuePair) {
        return PutSchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(schemaVersionId.toString())
                .metadataKeyValue(metadataKeyValuePair)
                .build();
    }

    private PutSchemaVersionMetadataResponse createPutSchemaVersionMetadataResponse(
            UUID schemaVersionId, MetadataKeyValuePair metadataKeyValuePair) {
        return PutSchemaVersionMetadataResponse
                .builder()
                .schemaVersionId(schemaVersionId.toString())
                .metadataKey(metadataKeyValuePair.metadataKey())
                .metadataValue(metadataKeyValuePair.metadataValue())
                .build();
    }

    private AWSSchemaRegistryClient configureAWSSchemaRegistryClientWithSerdeConfig(AWSSchemaRegistryClient awsSchemaRegistryClient,
                                                                                    GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration)
            throws NoSuchFieldException, IllegalAccessException {
        Field serdeConfigField = AWSSchemaRegistryClient.class.getDeclaredField("glueSchemaRegistryConfiguration");
        serdeConfigField.setAccessible(true);
        serdeConfigField.set(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);

        return awsSchemaRegistryClient;
    }
}
