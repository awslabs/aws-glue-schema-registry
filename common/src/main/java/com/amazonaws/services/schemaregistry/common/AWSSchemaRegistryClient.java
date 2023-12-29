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
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.urlconnection.ProxyConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.CreateSchemaRequest;
import software.amazon.awssdk.services.glue.model.CreateSchemaResponse;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaByDefinitionResponse;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.GetTagsRequest;
import software.amazon.awssdk.services.glue.model.GetTagsResponse;
import software.amazon.awssdk.services.glue.model.GlueRequest;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.MetadataKeyValuePair;
import software.amazon.awssdk.services.glue.model.MetadataInfo;
import software.amazon.awssdk.services.glue.model.ListSchemaVersionsResponse;
import software.amazon.awssdk.services.glue.model.ListSchemaVersionsRequest;
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.PutSchemaVersionMetadataResponse;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataRequest;
import software.amazon.awssdk.services.glue.model.QuerySchemaVersionMetadataResponse;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionRequest;
import software.amazon.awssdk.services.glue.model.RegisterSchemaVersionResponse;
import software.amazon.awssdk.services.glue.model.RegistryId;
import software.amazon.awssdk.services.glue.model.SchemaId;
import software.amazon.awssdk.services.glue.model.SchemaVersionListItem;
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient;
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.GetResourcesRequest;
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.GetResourcesResponse;
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.TagFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringJoiner;
import java.util.UUID;

/**
 * Handles all the requests related to the schema management.
 */
@Slf4j
public class AWSSchemaRegistryClient {

    private static final int MAX_ATTEMPTS = 10;
    private static final long MAX_WAIT_INTERVAL = 3000;

    private final GlueClient client;
    private GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration;

    private ResourceGroupsTaggingApiClient taggingApiClient;

    /**
     * Create Amazon Schema Registry Client.
     *
     * @param credentialsProvider           credentials provider
     * @param glueSchemaRegistryConfiguration schema registry configuration elements
     * @throws AWSSchemaRegistryException on any error while building the client
     */
    public AWSSchemaRegistryClient(@NonNull AwsCredentialsProvider credentialsProvider,
                                   @NonNull GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration,
                                   @NonNull RetryPolicy retryPolicy) {
        this.glueSchemaRegistryConfiguration = glueSchemaRegistryConfiguration;
        ClientOverrideConfiguration overrideConfiguration = ClientOverrideConfiguration.builder()
                .retryPolicy(retryPolicy)
                .addExecutionInterceptor(new UserAgentRequestInterceptor())
                .build();
        UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder = UrlConnectionHttpClient.builder();
        if (glueSchemaRegistryConfiguration.getProxyUrl() != null) {
        	log.debug("Creating http client using proxy {}", glueSchemaRegistryConfiguration.getProxyUrl().toString());
    		ProxyConfiguration proxy = ProxyConfiguration.builder().endpoint(glueSchemaRegistryConfiguration.getProxyUrl()).build();
    		urlConnectionHttpClientBuilder.proxyConfiguration(proxy);
        }

        GlueClientBuilder glueClientBuilder = GlueClient
                .builder()
                .credentialsProvider(credentialsProvider)
                .overrideConfiguration(overrideConfiguration)
                .httpClient(urlConnectionHttpClientBuilder.build())
                .region(Region.of(glueSchemaRegistryConfiguration.getRegion()));

        if (glueSchemaRegistryConfiguration.getEndPoint() != null) {
            try {
                glueClientBuilder.endpointOverride(new URI(glueSchemaRegistryConfiguration.getEndPoint()));
            } catch (URISyntaxException e) {
                String message = String.format("Malformed uri, please pass the valid uri for creating the client",
                                               glueSchemaRegistryConfiguration.getEndPoint());
                throw new AWSSchemaRegistryException(message, e);
            }
        }
        this.client = glueClientBuilder.build();
    }

    /**
     * Create Amazon Schema Registry Client.
     *
     * @param credentialsProvider           credentials provider
     * @param glueSchemaRegistryConfiguration schema registry configuration elements
     * @throws AWSSchemaRegistryException on any error while building the client
     */
    public AWSSchemaRegistryClient(@NonNull AwsCredentialsProvider credentialsProvider,
                                   @NonNull GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration) {
        this(credentialsProvider, glueSchemaRegistryConfiguration, RetryPolicy.defaultRetryPolicy());
    }

    public AWSSchemaRegistryClient(@NonNull GlueClient glueClient) {
        this.client = glueClient;
    }

    /**
     * Get Schema Version ID by passing the schema definition.
     * @param schemaDefinition Schema Definition
     * @param schemaName       Schema Name
     * @param dataFormat       Data Format
     * @return                 Schema Version ID
     * @throws AWSSchemaRegistryException on any error while fetching the schema version ID
     */
    public UUID getSchemaVersionIdByDefinition(@NonNull String schemaDefinition, @NonNull String schemaName,
                                               @NonNull String dataFormat) throws AWSSchemaRegistryException {
        try {
            String message = String.format(
                    "Getting Schema Version Id for : schemaDefinition = %s, schemaName = %s, dataFormat = %s",
                    schemaDefinition, schemaName, dataFormat);
            log.debug(message);
            GetSchemaByDefinitionResponse response = null;
            response = client.getSchemaByDefinition(buildGetSchemaByDefinitionRequest(schemaDefinition, schemaName));
            return returnSchemaVersionIdIfAvailable(response);
        } catch (Exception e) {
            String message = String.format("Failed to get schemaVersionId by schema definition for schema name = %s ", schemaName);
            throw new AWSSchemaRegistryException(message, e);
        }
    }

    /**
     * Get the schema definition by passing the schema id.
     *
     * @param schemaVersionId schema version id
     * @return                schema definition returns the schema definition corresponding to the
     *                        schema id passed and null in case service is not able to found the
     *                        schema definition corresponding to schema id.
     * @throws AWSSchemaRegistryException on any errors during schema retrieval from service
     */
    public GetSchemaVersionResponse getSchemaVersionResponse(@NonNull String schemaVersionId)
            throws AWSSchemaRegistryException {
        GetSchemaVersionResponse schemaVersionResponse = null;

        try {
            schemaVersionResponse = client.getSchemaVersion(getSchemaVersionRequest(schemaVersionId));
            validateSchemaVersionResponse(schemaVersionResponse, schemaVersionId);
        } catch (Exception e) {
            String errorMessage = String.format("Failed to get schema version Id = %s", schemaVersionId);
            throw new AWSSchemaRegistryException(errorMessage, e);
        }

        return schemaVersionResponse;
    }

    private GetSchemaVersionRequest getSchemaVersionRequest(String schemaVersionId) {
        GetSchemaVersionRequest getSchemaVersionRequest = GetSchemaVersionRequest.builder()
                .schemaVersionId(schemaVersionId).build();
        return getSchemaVersionRequest;
    }

    private void validateSchemaVersionResponse(GetSchemaVersionResponse schemaVersionResponse, String schemaVersionId) {
        if (schemaVersionResponse == null || schemaVersionResponse.schemaVersionId() == null) {
            String message = String.format("Schema definition is not present for the schema id = %s", schemaVersionId);
            throw new AWSSchemaRegistryException(message);
        }
    }

    private UUID returnSchemaVersionIdIfAvailable(GetSchemaByDefinitionResponse response) {
        if (response.schemaVersionId() != null
                && response.statusAsString().equals(AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString())) {
            return UUID.fromString(response.schemaVersionId());
        } else {
            String msg = String.format("Schema Found but status is %s", response.statusAsString());
            throw new AWSSchemaRegistryException(msg);
        }
    }

    /**
     * Create a request to get a schema using the schema definition and the schema name.
     *
     * @param schemaDefinition Schema Definition
     * @param schemaName       Schema Name
     * @return                 GetSchemaByDefinitionRequest object
     */
    public GetSchemaByDefinitionRequest buildGetSchemaByDefinitionRequest(String schemaDefinition, String schemaName) {
        return buildGetSchemaByDefinitionRequest(schemaDefinition, schemaName, glueSchemaRegistryConfiguration.getRegistryName());
    }

    /**
     * Create a request to get a schema using the schema definition and the schema name.
     *
     * @param schemaDefinition Schema Definition
     * @param schemaName       Schema Name
     * @param registryName     Registry name
     * @return                 GetSchemaByDefinitionRequest object
     */
    public GetSchemaByDefinitionRequest buildGetSchemaByDefinitionRequest(String schemaDefinition, String schemaName,
                                                                          String registryName) {
        GetSchemaByDefinitionRequest request = GetSchemaByDefinitionRequest.builder()
                .schemaId(getSchemaIdRequestObject(schemaName, registryName))
                .schemaDefinition(schemaDefinition).build();
        return request;
    }

    /**
     * Create a schema using the Glue client and return the response object
     * @param schemaName Schema Name
     * @param dataFormat Data Format
     * @param schemaDefinition Schema Definition
     * @param metadata schema version metadata
     * @return           CreateSchemaResponse object
     * @throws AWSSchemaRegistryException on any error during the schema creation
     */
    public UUID createSchema(String schemaName,
                             String dataFormat,
                             String schemaDefinition,
                             Map<String, String> metadata) throws AWSSchemaRegistryException {
        UUID schemaVersionId = null;
        try {
            log.info("Auto Creating schema with schemaName: {} and schemaDefinition : {}", schemaName,
                      schemaDefinition);
            CreateSchemaResponse createSchemaResponse =
                    client.createSchema(getCreateSchemaRequestObject(schemaName, dataFormat, schemaDefinition));
            schemaVersionId = UUID.fromString(createSchemaResponse.schemaVersionId());
        } catch (AlreadyExistsException e) {
            log.warn("Schema is already created, this could be caused by multiple producers racing to "
                     + "auto-create schema.");
            schemaVersionId = registerSchemaVersion(schemaDefinition, schemaName, dataFormat, metadata);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Create schema :: Call failed when creating the schema with the schema registry for"
                    + " schema name = %s", schemaName);
            throw new AWSSchemaRegistryException(errorMessage, e);
        }

        putSchemaVersionMetadata(schemaVersionId, metadata);

        return schemaVersionId;
    }

    /**
     * Register the schema and return schema version Id once it is available.
     * @param schemaDefinition Schema Definition
     * @param schemaName       Schema Name
     * @param dataFormat       Data Format
     * @param metadata         Metadata Map
     * @return                 Unique schema version ID.
     * @throws AWSSchemaRegistryException on any error during the registration and fetching of schema version
     */
    public UUID registerSchemaVersion(String schemaDefinition, String schemaName, String dataFormat, Map<String, String> metadata) {
        GetSchemaVersionResponse getSchemaVersionResponse = registerSchemaVersion(schemaDefinition, schemaName, dataFormat);
        UUID schemaVersionId = UUID.fromString(getSchemaVersionResponse.schemaVersionId());
        putSchemaVersionMetadata(schemaVersionId, metadata);

        return schemaVersionId;
    }

    /**
     * Register the schema and return get schema version response once it is available.
     * @param schemaDefinition Schema Definition
     * @param schemaName       Schema Name
     * @param dataFormat       Data Format
     * @return GetSchemaVersionResponse object.
     * @throws AWSSchemaRegistryException on any error during the registration and fetching of schema version
     */
    public GetSchemaVersionResponse registerSchemaVersion(String schemaDefinition, String schemaName, String dataFormat) throws AWSSchemaRegistryException {

        GetSchemaVersionResponse schemaVersionResponse = null;

        try {
            RegisterSchemaVersionResponse registerSchemaVersionResponse =
                    client.registerSchemaVersion(getRegisterSchemaVersionRequest(schemaDefinition, schemaName));

            log.info("Registered the schema version with schema version id = {} and with version number = {} and "
                     + "status {}", registerSchemaVersionResponse.schemaVersionId(),
                     registerSchemaVersionResponse.versionNumber(), registerSchemaVersionResponse.statusAsString());

            if (AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString()
                    .equals(registerSchemaVersionResponse.statusAsString())) {
                return transformToGetSchemaVersionResponse(registerSchemaVersionResponse);
            }

            schemaVersionResponse = waitForSchemaEvolutionCheckToComplete(
                    getGetSchemaVersionRequest(registerSchemaVersionResponse.schemaVersionId()));

        } catch (Exception e) {
            String errorMessage = String.format("Register schema :: Call failed when registering the schema with the schema registry for schema name = %s",
                    schemaName);
            throw new AWSSchemaRegistryException(errorMessage, e);
        }

        return schemaVersionResponse;
    }

    private GetSchemaVersionResponse transformToGetSchemaVersionResponse(RegisterSchemaVersionResponse registerSchemaVersionResponse) {
        return GetSchemaVersionResponse.builder()
                .schemaVersionId(registerSchemaVersionResponse.schemaVersionId())
                .status(registerSchemaVersionResponse.status())
                .status(registerSchemaVersionResponse.statusAsString())
                .versionNumber(registerSchemaVersionResponse.versionNumber())
                .build();
    }

    private CreateSchemaRequest getCreateSchemaRequestObject(String schemaName, String dataFormat, String schemaDefinition) {
        return CreateSchemaRequest
                .builder()
                .dataFormat(DataFormat.valueOf(dataFormat))
                .description(glueSchemaRegistryConfiguration.getDescription())
                .registryId(RegistryId.builder().registryName(glueSchemaRegistryConfiguration.getRegistryName()).build())
                .schemaName(schemaName)
                .schemaDefinition(schemaDefinition)
                .compatibility(glueSchemaRegistryConfiguration.getCompatibilitySetting())
                .tags(glueSchemaRegistryConfiguration.getTags())
                .build();
    }

    private RegisterSchemaVersionRequest getRegisterSchemaVersionRequest(String schemaDefinition, String schemaName) {
        return RegisterSchemaVersionRequest
                .builder()
                .schemaDefinition(schemaDefinition)
                .schemaId(getSchemaIdRequestObject(schemaName, glueSchemaRegistryConfiguration.getRegistryName()))
                .build();
    }

    private SchemaId getSchemaIdRequestObject(@NonNull String schemaName, @NonNull String registryName) {
        return SchemaId
                .builder()
                .schemaName(schemaName)
                .registryName(registryName)
                .build();
    }

    private GetSchemaVersionRequest getGetSchemaVersionRequest(String schemaVersionId) {
        return GetSchemaVersionRequest
                .builder()
                .schemaVersionId(schemaVersionId)
                .build();
    }

    /**
     * Get schema version response of asynchronous operation.
     *
     * @return Schema version.
     */
    private GetSchemaVersionResponse waitForSchemaEvolutionCheckToComplete(GetSchemaVersionRequest getSchemaVersionRequest) {

        GetSchemaVersionResponse response;

        try {
            int retries = 0;

            Thread.sleep(MAX_WAIT_INTERVAL);

            do {
                response = client.getSchemaVersion(getSchemaVersionRequest);

                if (AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString()
                        .equals(response.statusAsString())) {
                    return response;
                } else if (!AWSSchemaRegistryConstants.SchemaVersionStatus.PENDING.toString()
                        .equals(response.statusAsString())) {
                    throw new AWSSchemaRegistryException(String.format("Schema evolution check failed. "
                                                                       + "schemaVersionId %s is in %s status.",
                                                                       getSchemaVersionRequest.schemaVersionId(),
                                                                       response.statusAsString()));
                }

            } while (retries++ < MAX_ATTEMPTS - 1);

            if (retries >= MAX_ATTEMPTS && !AWSSchemaRegistryConstants.SchemaVersionStatus.AVAILABLE.toString()
                    .equals(response.statusAsString())) {
                throw new AWSSchemaRegistryException(String.format("Retries exhausted for schema evolution check for "
                                                                   + "schemaVersionId = %s",
                                                                   getSchemaVersionRequest.schemaVersionId()));
            }
        } catch (Exception ex) {
            String message =
                    String.format("Exception occurred, while performing schema evolution check for schemaVersionId = "
                                  + "%s", getSchemaVersionRequest.schemaVersionId());
            throw new AWSSchemaRegistryException(message, ex);
        }
        return response;
    }

    /**
     * Put metadata to schema version asynchronously
     * @param schemaVersionId Schema Version Id
     * @param metadata Metadata Map
     */
    public void putSchemaVersionMetadata(UUID schemaVersionId, Map<String, String> metadata) {
        metadata.entrySet()
                .parallelStream()
                .map(this::createMetadataKeyValuePair)
                .forEach((metadataKeyValuePair -> {
                    try {
                        putSchemaVersionMetadata(schemaVersionId, metadataKeyValuePair);
                    } catch (AWSSchemaRegistryException e) {
                        log.warn(e.getMessage());
                    }
                }));
    }

    /**
     * Put metadata to schema version and return the response object
     * @param schemaVersionId Schema Version Id
     * @param metadataKeyValuePair Metadata Key Value Pair
     * @return           PutSchemaVersionMetadataResponse object
     * @throws AWSSchemaRegistryException on any error during putting metadata
     */
    public PutSchemaVersionMetadataResponse putSchemaVersionMetadata(UUID schemaVersionId, MetadataKeyValuePair metadataKeyValuePair)
            throws AWSSchemaRegistryException {
        PutSchemaVersionMetadataResponse response = null;
        try {
            response =
                    client.putSchemaVersionMetadata(createPutSchemaVersionMetadataRequest(schemaVersionId, metadataKeyValuePair));
        } catch (Exception e) {
            String errorMessage =
                    String.format("Put schema version metadata :: Call failed when put metadata key = %s value = %s to schema for schema version id = %s",
                            metadataKeyValuePair.metadataKey(), metadataKeyValuePair.metadataValue(), schemaVersionId.toString());
            throw new AWSSchemaRegistryException(errorMessage, e);
        }
        return response;
    }

    private PutSchemaVersionMetadataRequest createPutSchemaVersionMetadataRequest(UUID schemaVersionId, MetadataKeyValuePair metadataKeyValuePair) {
        return PutSchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(schemaVersionId.toString())
                .metadataKeyValue(metadataKeyValuePair)
                .build();
    }

    private MetadataKeyValuePair createMetadataKeyValuePair(Map.Entry<String, String> metadataEntry) {
        return MetadataKeyValuePair
                .builder()
                .metadataKey(metadataEntry.getKey())
                .metadataValue(metadataEntry.getValue())
                .build();
    }

    /**
     * Query metadata for schema version and return the response object
     *
     * @param schemaVersionId Schema Version Id
     * @return QuerySchemaVersionMetadataResponse object
     * @throws AWSSchemaRegistryException on any error during putting metadata
     */
    public QuerySchemaVersionMetadataResponse querySchemaVersionMetadata(UUID schemaVersionId) {
        QuerySchemaVersionMetadataResponse response = null;
        try {
            response = client.querySchemaVersionMetadata(createQuerySchemaVersionMetadataRequest(schemaVersionId));
        } catch (Exception e) {
            String errorMessage = String.format("Query schema version metadata :: Call failed when query metadata for schema version id = %s",
                    schemaVersionId.toString());
            throw new AWSSchemaRegistryException(errorMessage, e);
        }

        return response;
    }

    private QuerySchemaVersionMetadataRequest createQuerySchemaVersionMetadataRequest(UUID schemaVersionId) {
        return QuerySchemaVersionMetadataRequest
                .builder()
                .schemaVersionId(schemaVersionId.toString())
                .build();
    }

    /**
     * Query Schema Tags Response for a given schema name and definition
     * @param schemaDefinition  Schema Definition
     * @param schemaName        Schema Name
     * @return a GetTagsResponse with tags
     */
    public GetTagsResponse querySchemaTags(String schemaDefinition, String schemaName) {
        GetTagsResponse getTagsResponse = null;
        try {
            GetSchemaByDefinitionResponse getSchemaByDefinitionResponse = client.getSchemaByDefinition(
                    buildGetSchemaByDefinitionRequest(schemaDefinition, schemaName));
            GetTagsRequest getTagsRequest = GetTagsRequest.builder()
                    .resourceArn(getSchemaByDefinitionResponse.schemaArn())
                    .build();

            getTagsResponse = client.getTags(getTagsRequest);
        } catch (Exception e) {
            String errorMessage = String.format("Query schema tags:: Call failed while querying tags for schema = %s", schemaName);
            throw new AWSSchemaRegistryException(errorMessage, e);
        }
        return getTagsResponse;
    }

    /**
     * AWS SDK Request interceptor that adds additional data to the UserAgent of Glue API requests.
     */
    @VisibleForTesting
    protected class UserAgentRequestInterceptor implements ExecutionInterceptor {
        private static final String ONE = "1";
        private static final String ZERO = "0";

        @Override
        public SdkRequest modifyRequest(Context.ModifyRequest context, ExecutionAttributes executionAttributes) {
            if (!(context.request() instanceof GlueRequest)) {
                //Only applies to Glue requests.
                return context.request();
            }

            GlueRequest request = (GlueRequest) context.request();
            AwsRequestOverrideConfiguration overrideConfiguration =
                request.overrideConfiguration().map(config ->
                    config
                        .toBuilder()
                        .addApiName(getApiName())
                        .build())
                    .orElse((AwsRequestOverrideConfiguration.builder()
                        .addApiName(getApiName())
                        .build()));

            return request.toBuilder().overrideConfiguration(overrideConfiguration).build();
        }

        private ApiName getApiName() {
            return ApiName.builder()
                .version(com.amazonaws.services.schemaregistry.common.MavenPackaging.VERSION)
                .name(buildUserAgentSuffix())
                .build();
        }

        private String buildUserAgentSuffix() {
            Map<String, String> userAgentSuffixItems = ImmutableMap.of(
                "autoreg", glueSchemaRegistryConfiguration.isSchemaAutoRegistrationEnabled() ? ONE : ZERO,
                "compress", glueSchemaRegistryConfiguration.getCompressionType().equals(
                    AWSSchemaRegistryConstants.COMPRESSION.ZLIB) ? ONE : ZERO,
                "secdeser", glueSchemaRegistryConfiguration.getSecondaryDeserializer() != null ? ONE : ZERO,
                "app", glueSchemaRegistryConfiguration.getUserAgentApp()
            );

            StringJoiner userAgentSuffix = new StringJoiner(":");

            userAgentSuffixItems
                .forEach((key, value) -> userAgentSuffix.add(key + "/" + value));

            return userAgentSuffix.toString();
        }
    }
    /**
     * Retrieves an instance of the ResourceGroupsTaggingApiClient.
     *
     * <p>
     * This method lazily initializes the taggingApiClient if it hasn't been created yet.
     * If a proxy URL is provided in the glueSchemaRegistryConfiguration, the client is configured
     * to use that proxy for its HTTP connections.
     * </p>
     *
     * @return the ResourceGroupsTaggingApiClient instance
     */

    private ResourceGroupsTaggingApiClient getTaggingApiClient() {
        if (taggingApiClient == null) {
            UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder = UrlConnectionHttpClient.builder();

            if (glueSchemaRegistryConfiguration.getProxyUrl() != null) {
                log.debug("Creating http client using proxy {}", glueSchemaRegistryConfiguration.getProxyUrl().toString());
                ProxyConfiguration proxy = ProxyConfiguration.builder().endpoint(glueSchemaRegistryConfiguration.getProxyUrl()).build();
                urlConnectionHttpClientBuilder.proxyConfiguration(proxy);
            }

            taggingApiClient = ResourceGroupsTaggingApiClient.builder()
                    .httpClient(urlConnectionHttpClientBuilder.build())
                    .build();
        }
        return taggingApiClient;
    }


    /**
     * Retrieve all schema ARNs based on the provided tag key and tag value.
     * @param tagKey The tag key to look up.
     * @param tagValue The tag value to match.
     * @return List of schema ARNs
     */
    public List<String> getSchemasWithTag(String tagKey, String tagValue) {
        ResourceGroupsTaggingApiClient taggingApiClient = getTaggingApiClient();
        List<String> schemaArns = new ArrayList<>();

        TagFilter tagFilter = TagFilter.builder()
                .key(tagKey)
                .values(tagValue)
                .build();

        GetResourcesRequest request = GetResourcesRequest.builder()
                .tagFilters(tagFilter)
                .build();

        GetResourcesResponse response = taggingApiClient.getResources(request);

        response.resourceTagMappingList().forEach(resourceTagMapping -> {
            schemaArns.add(resourceTagMapping.resourceARN());
        });

        return schemaArns;
    }


    /**
     * List all schema versions for a given schema name.
     *
     * @param schemaVersionIdTagKey tag key name, schemaId, schemaVersionId
     * @return schemaVersionId matched with the tag key name
     * @throws AWSSchemaRegistryException on any error during the listing of schema versions
     */

    public UUID filterSchemaVersionByTag(String schemaVersionIdTagKey, SchemaId schemaId, UUID schemaVersionId) {
        try {
            ListSchemaVersionsRequest listSchemaVersionsRequest = ListSchemaVersionsRequest.builder()
                    .schemaId(schemaId)
                    .build();

            ListSchemaVersionsResponse listSchemaVersionsResponse = client.listSchemaVersions(listSchemaVersionsRequest);

            for (SchemaVersionListItem schemaVersion : listSchemaVersionsResponse.schemas()) {
                QuerySchemaVersionMetadataRequest metadataRequest = QuerySchemaVersionMetadataRequest.builder()
                        .schemaVersionId(schemaVersion.schemaVersionId())
                        .build();

                QuerySchemaVersionMetadataResponse metadataResponse = client.querySchemaVersionMetadata(metadataRequest);
                Map<String, MetadataInfo> metadataMap = metadataResponse.metadataInfoMap();
                MetadataInfo schemaIdMetadata = metadataMap.get(schemaVersionIdTagKey);
                if (schemaIdMetadata != null && schemaIdMetadata.metadataValue().equals(schemaVersionId.toString())) {
                    return UUID.fromString(schemaVersion.schemaVersionId());
                }
            }
            return null;
        } catch (GlueException e) {
            String errorMessage = "Error occurred while filtering schema version by tag: " + e.getMessage();
            throw new RuntimeException(errorMessage, e);
        }
    }
}