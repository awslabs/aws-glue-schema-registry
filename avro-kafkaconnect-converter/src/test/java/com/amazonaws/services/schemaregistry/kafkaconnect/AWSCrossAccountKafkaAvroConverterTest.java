package com.amazonaws.services.schemaregistry.kafkaconnect;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants.ASSUME_ROLE_SESSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AWSCrossAccountKafkaAvroConverterTest {

    private static final String ROLE_ARN = "arn:aws:iam::123456789012:role/my-role";
    private static final String REGION = "eu-west-1";

    @Mock
    private AwsCredentialsProvider mockCredProvider;
    @Spy
    private AWSCrossAccountKafkaAvroConverter converter;

    private Map<String, Object> baseConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(AWSSchemaRegistryConstants.ASSUME_ROLE_ARN, ROLE_ARN);
        props.put(AWSSchemaRegistryConstants.AWS_REGION, REGION);
        props.put(AWSSchemaRegistryConstants.AWS_ENDPOINT, "https://test");
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, "GENERIC_RECORD");
        return props;
    }

    @Test
    void configureThrowsIfAssumeRoleArnMissing() {
        assertThrows(
                AWSSchemaRegistryException.class,
                () -> converter.configure(new HashMap<>(), false),
                AWSSchemaRegistryConstants.ASSUME_ROLE_ARN + " is not defined in the properties"
        );
    }

    @Test
    void configureInitializesSerializerDeserializerAvroDataAndKeyFlag() {
        Map<String, Object> configs = baseConfigs();

        converter.configure(configs, false);
        assertFalse(converter.isKey());
        assertNotNull(converter.getSerializer());
        assertNotNull(converter.getDeserializer());
        assertNotNull(converter.getAvroData());

        converter.configure(configs, true);
        assertTrue(converter.isKey());
    }

    @Test
    void configureInvokesAssumeRoleWithCustomSession() {
        Map<String,Object> configs = baseConfigs();
        configs.put(ASSUME_ROLE_SESSION, "my-session");

        doReturn(mockCredProvider)
                .when(converter)
                .getCredentialsProvider(anyString(), anyString(), anyString());

        converter.configure(configs, true);

        verify(converter).getCredentialsProvider(ROLE_ARN, "my-session", REGION);

        assertTrue(converter.isKey());
        assertNotNull(converter.getSerializer());
        assertNotNull(converter.getDeserializer());
        assertNotNull(converter.getAvroData());
    }

    @Test
    void configureUsesDefaultSessionNameWhenMissing() {
        Map<String,Object> configs = baseConfigs();

        doReturn(mockCredProvider)
                .when(converter)
                .getCredentialsProvider(anyString(), anyString(), anyString());

        converter.configure(configs, false);

        verify(converter).getCredentialsProvider(ROLE_ARN, "kafka-connect-session", REGION);
        assertFalse(converter.isKey());
    }
}