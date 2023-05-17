package com.amazonaws.services.schemaregistry.common;

import com.amazonaws.services.schemaregistry.common.MavenPackaging;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.ApiName;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.glue.model.GetSchemaVersionRequest;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class UserAgentRequestInterceptorTest {

    public static final String REGION = "us-east-1";

    private static Stream<Arguments> getClientConfigTestCases() {
        return Stream.of(
            Arguments.of(
                new GlueSchemaRegistryConfiguration(
                    ImmutableMap.of(
                        AWSSchemaRegistryConstants.AWS_REGION, REGION,
                        AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true,
                        AWSSchemaRegistryConstants.COMPRESSION_TYPE,
                        AWSSchemaRegistryConstants.COMPRESSION.ZLIB.toString(),
                        AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, GlueSchemaRegistryDataFormatSerializer.class,
                        AWSSchemaRegistryConstants.USER_AGENT_APP, UserAgents.KAFKA
                    )
                ), "autoreg/1:compress/1:secdeser/1:app/kafka"),
            Arguments.of(
                new GlueSchemaRegistryConfiguration(
                    ImmutableMap.of(
                        AWSSchemaRegistryConstants.AWS_REGION, REGION,
                        AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, false,
                        AWSSchemaRegistryConstants.COMPRESSION_TYPE,
                        AWSSchemaRegistryConstants.COMPRESSION.NONE.toString()
                    )
                ), "autoreg/0:compress/0:secdeser/0:app/default")
        );
    }

    @ParameterizedTest
    @MethodSource("getClientConfigTestCases")
    void test_UserAgentInterceptor_ReturnsSdkRequestWithUserAgent(GlueSchemaRegistryConfiguration config,
        String expectedName) {
        AwsCredentialsProvider mockAwsCredentialsProvider = mock(AwsCredentialsProvider.class);

        AWSSchemaRegistryClient awsSchemaRegistryClient =
            new AWSSchemaRegistryClient(mockAwsCredentialsProvider, config);

        AWSSchemaRegistryClient.UserAgentRequestInterceptor userAgentRequestInterceptor =
            awsSchemaRegistryClient.new UserAgentRequestInterceptor();

        Context.ModifyRequest modifyRequest = mock(Context.ModifyRequest.class);
        GetSchemaVersionRequest glueRequest = GetSchemaVersionRequest.builder().build();
        doReturn(glueRequest).when(modifyRequest).request();

        SdkRequest sdkHttpRequest = userAgentRequestInterceptor.modifyRequest(modifyRequest, null);
        assertNotNull(sdkHttpRequest);
        assertTrue(sdkHttpRequest.overrideConfiguration().isPresent());

        ApiName actualApiName = sdkHttpRequest.overrideConfiguration().get().apiNames().get(0);

        assertEquals(MavenPackaging.VERSION, actualApiName.version());
        assertEquals(expectedName, actualApiName.name());
    }

    @Test
    void test_UserAgentInterceptor_ReturnsSameRequestForNonGlueRequests() {
        AwsCredentialsProvider mockAwsCredentialsProvider = mock(AwsCredentialsProvider.class);

        AWSSchemaRegistryClient awsSchemaRegistryClient =
            new AWSSchemaRegistryClient(mockAwsCredentialsProvider, new GlueSchemaRegistryConfiguration(REGION));

        AWSSchemaRegistryClient.UserAgentRequestInterceptor userAgentRequestInterceptor =
            awsSchemaRegistryClient.new UserAgentRequestInterceptor();

        SdkRequest nonGlueRequest = mock(SdkRequest.class);
        Context.ModifyRequest modifyRequest = mock(Context.ModifyRequest.class);
        doReturn(nonGlueRequest).when(modifyRequest).request();

        SdkRequest actualRequest = userAgentRequestInterceptor.modifyRequest(modifyRequest, null);

        assertEquals(nonGlueRequest, actualRequest);
    }
}