package com.amazonaws.services.schemaregistry.config;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.Map;
import java.util.Properties;
@Slf4j
@Data
public class NativeGlueSchemaRegistryConfiguration extends GlueSchemaRegistryConfiguration {
    private String roleToAssume;
    private String roleSessionName; // should only be set if roleToAssumeIsSet

    public static final String ROLE_TO_ASSUME_KEY = "roleToAssume";
    public static final String ROLE_SESSION_NAME_KEY = "roleSessionName";
    public static final String USER_AGENT_APP_KEY = "userAgentApp";

    public NativeGlueSchemaRegistryConfiguration(String region) {
        super(region);
        this.setUserAgentApp("native-glue-schema-registry");
    }

    public NativeGlueSchemaRegistryConfiguration(Map<String, ?> configs) {
        super(configs);
        validateAndSetRoleConfiguration(configs);
        validateAndSetUserAgentApp(configs);
    }

    public NativeGlueSchemaRegistryConfiguration(Properties properties) {
        super(properties);
        final Map<String, ?> configs = getMapFromPropertiesFile(properties);
        validateAndSetRoleConfiguration(configs);
        validateAndSetUserAgentApp(configs);
    }

    private void validateAndSetRoleConfiguration(Map<String, ?> configs) {
        if (configs.containsKey("roleToAssume")) {
            this.roleToAssume = (String) configs.get("roleToAssume");
            this.roleSessionName = "native-glue-schema-registry";
        }
        if (configs.containsKey("roleSessionName")) {
            this.roleSessionName = (String) configs.get("roleSessionName"); // this will override the default session
                                                                            // name if there is a roleSessionName
                                                                            // defined by user
        }
    }

    private void validateAndSetUserAgentApp(Map<String, ?> configs) {
        if (configs.containsKey("userAgentApp")) {
            final String userAgentApp = (String) configs.get("userAgentApp");
            this.setUserAgentApp(userAgentApp);
        } else {
            this.setUserAgentApp("native-glue-schema-registry");
        }
    }

    public AwsCredentialsProvider getAwsCredentialsProvider() {
        AwsCredentialsProvider credentialsProvider;
        final DefaultCredentialsProvider defaultCredentialsProvider = DefaultCredentialsProvider.builder().build();

        if (this.getRoleToAssume() != null) {
            // Create STS assume role credentials provider using default credentials as base
            credentialsProvider = StsAssumeRoleCredentialsProvider.builder()
                .refreshRequest(AssumeRoleRequest.builder()
                    .roleArn(this.getRoleToAssume())
                    .roleSessionName(this.getRoleSessionName())
                    .build())
                .stsClient(StsClient.builder()
                    .httpClientBuilder(UrlConnectionHttpClient.builder())
                    .credentialsProvider(defaultCredentialsProvider)
                    .region(Region.of(this.getRegion()))
                    .build())
                .build();
        } else {
            credentialsProvider = defaultCredentialsProvider;
        }
        return credentialsProvider;
    }
}
