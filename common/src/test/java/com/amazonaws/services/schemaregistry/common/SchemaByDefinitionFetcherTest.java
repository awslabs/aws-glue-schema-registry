package com.amazonaws.services.schemaregistry.common;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.cache.LoadingCache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest;
import software.amazon.awssdk.services.glue.model.CreateSchemaResponse;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.SchemaId;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SchemaByDefinitionFetcherTest {
    private static final UUID SCHEMA_ID_FOR_TESTING = UUID.fromString("f8b4a7f0-9c96-4e4a-a687-fb5de9ef0c63");
    private AWSSchemaRegistryClient awsSchemaRegistryClient;
    private SchemaByDefinitionFetcher schemaByDefinitionFetcher;

    private GlueClient mockGlueClient;
    private String userSchemaDefinition;

    @BeforeEach
    void setUp() {
        mockGlueClient = mock(GlueClient.class);
        awsSchemaRegistryClient = new AWSSchemaRegistryClient(mockGlueClient);
        GlueSchemaRegistryConfiguration config = new GlueSchemaRegistryConfiguration(getConfigsWithAutoRegistrationSetting(true));
        schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(awsSchemaRegistryClient, config);
        userSchemaDefinition = "{Some-avro-schema}";
    }

    @Test
    public void testGetORRegisterSchemaVersionId_schemaVersionNotPresent_autoRegistersSchemaVersion() throws Exception {
        Map<String, String> configs = getConfigsWithAutoRegistrationSetting(true);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
            glueSchemaRegistryConfiguration);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
            .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);

        EntityNotFoundException entityNotFoundException =
            EntityNotFoundException.builder().message(AWSSchemaRegistryConstants.SCHEMA_VERSION_NOT_FOUND_MSG)
                .build();
        AWSSchemaRegistryException awsSchemaRegistryException = new AWSSchemaRegistryException(entityNotFoundException);

        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest)).thenThrow(awsSchemaRegistryException);

        Long schemaVersionNumber = 1L;
        SchemaId requestSchemaId = SchemaId.builder().schemaName(schemaName).registryName(registryName).build();

        RegisterSchemaVersionRequest registerSchemaVersionRequest = RegisterSchemaVersionRequest.builder()
            .schemaDefinition(userSchemaDefinition)
            .schemaId(requestSchemaId)
            .build();
        RegisterSchemaVersionResponse registerSchemaVersionResponse = RegisterSchemaVersionResponse.builder()
            .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
            .versionNumber(schemaVersionNumber)
            .build();
        when(mockGlueClient.registerSchemaVersion(registerSchemaVersionRequest))
            .thenReturn(registerSchemaVersionResponse);

        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder()
            .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
            .build();
        GetSchemaVersionResponse getSchemaVersionResponse = GetSchemaVersionResponse.builder()
            .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
            .schemaDefinition(userSchemaDefinition)
            .status(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())
            .build();
        when(mockGlueClient.getSchemaVersion(getSchemaVersionRequest)).thenReturn(getSchemaVersionResponse);

        schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);

        UUID schemaVersionId =
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata());

        assertEquals(SCHEMA_ID_FOR_TESTING, schemaVersionId);
    }

    @Test
    public void testGetORRegisterSchemaVersionId_nullSchemaDefinition_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> schemaByDefinitionFetcher
            .getORRegisterSchemaVersionId(null, "test-schema-name", DataFormat.AVRO.name(), getMetadata()));
    }

    @Test
    public void testGetORRegisterSchemaVersionId_nullSchemaSchemaName_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> schemaByDefinitionFetcher
            .getORRegisterSchemaVersionId(userSchemaDefinition, null, DataFormat.AVRO.name(), getMetadata()));
    }

    @Test
    public void testGetORRegisterSchemaVersionId_nullSchemaDataFormat_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> schemaByDefinitionFetcher
            .getORRegisterSchemaVersionId(userSchemaDefinition, "", null, getMetadata()));
    }

    @Test
    public void testGetORRegisterSchemaVersionId_nullMetadata_throwsException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> schemaByDefinitionFetcher
            .getORRegisterSchemaVersionId(userSchemaDefinition, "", DataFormat.AVRO.toString(), null));
    }

    @Test
    public void testGetORRegisterSchemaVersionId_WhenVersionIsPresent_ReturnsIt() throws Exception {
        Map<String, String> configs = getConfigsWithAutoRegistrationSetting(true);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();

        GlueSchemaRegistryConfiguration awsSchemaRegistrySerDeConfigs = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
            .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);

        GetSchemaByDefinitionResponse getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(software.amazon.awssdk.services.glue.model.SchemaVersionStatus.AVAILABLE)
                .build();

        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenReturn(getSchemaByDefinitionResponse);

        schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs);

        UUID schemaVersionId =
            schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata());

        assertEquals(SCHEMA_ID_FOR_TESTING, schemaVersionId);
    }

    @Test
    public void testGetORRegisterSchemaVersionId_OnUnknownException_ThrowsException() throws Exception {
        Map<String, String> configs = getConfigsWithAutoRegistrationSetting(true);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();

        GlueSchemaRegistryConfiguration awsSchemaRegistrySerDeConfigs = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
            .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);

        AWSSchemaRegistryException awsSchemaRegistryException =
            new AWSSchemaRegistryException(new RuntimeException("Unknown"));

        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest)).thenThrow(awsSchemaRegistryException);

        schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(awsSchemaRegistryClient, awsSchemaRegistrySerDeConfigs);

        Exception exception = assertThrows(AWSSchemaRegistryException.class,
            () -> schemaByDefinitionFetcher
                .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata()));
        assertTrue(
            exception.getMessage().contains("Exception occurred while fetching or registering schema definition"));
    }

    @Test
    public void testGetORRegisterSchemaVersionId_schemaNotPresent_autoCreatesSchema() throws Exception {
        Map<String, String> configs = getConfigsWithAutoRegistrationSetting(true);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
            glueSchemaRegistryConfiguration);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
            .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);

        EntityNotFoundException entityNotFoundException =
            EntityNotFoundException.builder().message(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG)
                .build();
        AWSSchemaRegistryException awsSchemaRegistryException = new AWSSchemaRegistryException(entityNotFoundException);

        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest)).thenThrow(awsSchemaRegistryException);

        CreateSchemaResponse createSchemaResponse = CreateSchemaResponse.builder()
            .schemaName(schemaName)
            .dataFormat(dataFormatName)
            .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
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

        schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);

        UUID schemaVersionId = schemaByDefinitionFetcher
            .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata());

        assertEquals(SCHEMA_ID_FOR_TESTING, schemaVersionId);
    }

    @Test
    public void testGetORRegisterSchemaVersionId_autoRegistrationDisabled_failsIfSchemaVersionNotPresent()
        throws Exception {
        Map<String, String> configs = getConfigsWithAutoRegistrationSetting(false);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();

        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        awsSchemaRegistryClient =
            configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient,
                glueSchemaRegistryConfiguration);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
            .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);


        EntityNotFoundException entityNotFoundException =
            EntityNotFoundException.builder().message(AWSSchemaRegistryConstants.SCHEMA_NOT_FOUND_MSG).build();
        AWSSchemaRegistryException awsSchemaRegistryException = new AWSSchemaRegistryException(entityNotFoundException);

        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest)).thenThrow(awsSchemaRegistryException);

        schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);

        Exception exception = assertThrows(AWSSchemaRegistryException.class, () -> schemaByDefinitionFetcher
            .getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata()));

        assertEquals(AWSSchemaRegistryConstants.AUTO_REGISTRATION_IS_DISABLED_MSG, exception.getMessage());
    }

    @Test
    public void testGetORRegisterSchemaVersionId_retrieveSchemaVersionId_schemaVersionIdIsCached() throws Exception {
        Map<String, String> configs = getConfigsWithAutoRegistrationSetting(false);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
            .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);

        GetSchemaByDefinitionResponse getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(software.amazon.awssdk.services.glue.model.SchemaVersionStatus.AVAILABLE)
                .build();

        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenReturn(getSchemaByDefinitionResponse);

        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);
        schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);
        LoadingCache<Schema, UUID> cache = schemaByDefinitionFetcher.schemaDefinitionToVersionCache;

        //Ensure cache is empty to start with.
        assertEquals(0, cache.size());

        //First call
        schemaByDefinitionFetcher.getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata());
        //Second call
        schemaByDefinitionFetcher.getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata());
        //Third call
        schemaByDefinitionFetcher.getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata());

        //Ensure cache is populated
        assertEquals(1, cache.size());

        Schema expectedSchema = new Schema(userSchemaDefinition, dataFormatName, schemaName);
        Map.Entry<Schema, UUID> cacheEntry = (Map.Entry<Schema, UUID>) cache.asMap().entrySet().toArray()[0];

        //Ensure cache entries are expected
        assertEquals(expectedSchema, cacheEntry.getKey());
        assertEquals(SCHEMA_ID_FOR_TESTING, cacheEntry.getValue());

        //Ensure only 1 call happened.
        verify(mockGlueClient, times(1)).getSchemaByDefinition(getSchemaByDefinitionRequest);
    }

    @Test
    public void testGetORRegisterSchemaVersionId_continuesToServeFromCache_WhenCallsFail() throws Exception {
        Map<String, String> configs = getConfigsWithAutoRegistrationSetting(false);

        String schemaName = configs.get(AWSSchemaRegistryConstants.SCHEMA_NAME);
        String registryName = configs.get(AWSSchemaRegistryConstants.REGISTRY_NAME);
        String dataFormatName = DataFormat.AVRO.name();
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);

        //Override TTL to 1s.
        glueSchemaRegistryConfiguration.setTimeToLiveMillis(1000L);

        GetSchemaByDefinitionRequest getSchemaByDefinitionRequest = awsSchemaRegistryClient
            .buildGetSchemaByDefinitionRequest(userSchemaDefinition, schemaName, registryName);

        GetSchemaByDefinitionResponse getSchemaByDefinitionResponse =
            GetSchemaByDefinitionResponse
                .builder()
                .schemaVersionId(SCHEMA_ID_FOR_TESTING.toString())
                .status(software.amazon.awssdk.services.glue.model.SchemaVersionStatus.AVAILABLE)
                .build();

        awsSchemaRegistryClient = configureAWSSchemaRegistryClientWithSerdeConfig(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);
        schemaByDefinitionFetcher = new SchemaByDefinitionFetcher(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);
        LoadingCache<Schema, UUID> cache = schemaByDefinitionFetcher.schemaDefinitionToVersionCache;

        //Ensure cache is empty to start with.
        assertEquals(0, cache.size());

        //Mock the client to return response, then fail and eventually succeed.
        when(mockGlueClient.getSchemaByDefinition(getSchemaByDefinitionRequest))
            .thenReturn(getSchemaByDefinitionResponse)
            .thenThrow(new RuntimeException("Service outage"))
            .thenThrow(new RuntimeException("Service outage"))
            .thenReturn(getSchemaByDefinitionResponse);

        //First call
        //As expected first call should fetch and cache the schema version.
        assertDoesNotThrow(() -> schemaByDefinitionFetcher.getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata()));
        assertEquals(1, cache.size());

        //Wait for 1.5 seconds to expire cache.
        Thread.sleep(1500L);

        //Second call shouldn't fail.
        assertDoesNotThrow(() -> schemaByDefinitionFetcher.getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata()));

        //Third call shouldn't fail.
        assertDoesNotThrow(() -> schemaByDefinitionFetcher.getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata()));

        //Verify the entry is not evicted.
        assertEquals(1, cache.size());

        //Fourth call shouldn't fail and cache is refreshed.
        assertDoesNotThrow(() -> schemaByDefinitionFetcher.getORRegisterSchemaVersionId(userSchemaDefinition, schemaName, dataFormatName, getMetadata()));
        verify(mockGlueClient, times(4)).getSchemaByDefinition(getSchemaByDefinitionRequest);
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

    private AWSSchemaRegistryClient configureAWSSchemaRegistryClientWithSerdeConfig(
        AWSSchemaRegistryClient awsSchemaRegistryClient,
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration)
        throws NoSuchFieldException, IllegalAccessException {
        Field serdeConfigField = AWSSchemaRegistryClient.class.getDeclaredField("glueSchemaRegistryConfiguration");
        serdeConfigField.setAccessible(true);
        serdeConfigField.set(awsSchemaRegistryClient, glueSchemaRegistryConfiguration);

        return awsSchemaRegistryClient;
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
}