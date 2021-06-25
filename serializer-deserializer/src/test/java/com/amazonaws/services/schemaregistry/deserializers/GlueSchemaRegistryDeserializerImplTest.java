package com.amazonaws.services.schemaregistry.deserializers;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;

@ExtendWith(MockitoExtension.class)
public class GlueSchemaRegistryDeserializerImplTest {
    @Mock
    private AwsCredentialsProvider credentialsProvider;

    @Mock
    private GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade;

    private static final String REGION = "us-west-2";
    private final static byte[] ENCODED_DATA = new byte[] { 8, 9, 12, 83, 82 };
    private final static Schema SCHEMA_REGISTRY_SCHEMA = new Schema("{}", "AVRO", "schemaFoo");
    private final Map<String, Object> config =
        ImmutableMap.of(
            AWSSchemaRegistryConstants.AWS_REGION, REGION
        );

    private GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer;

    @BeforeEach
    void setUp() {
        glueSchemaRegistryDeserializer = new GlueSchemaRegistryDeserializerImpl(glueSchemaRegistryDeserializationFacade);
    }

    @Test
    public void instantiate_WithConfig_CreatesInstance() {
        GlueSchemaRegistryConfiguration configuration = new GlueSchemaRegistryConfiguration(REGION);
        GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer =
            new GlueSchemaRegistryDeserializerImpl(credentialsProvider, configuration);

        assertNotNull(glueSchemaRegistryDeserializer);
    }

    @Test
    public void getData_WhenValidSchemaRegistryEncodedBytesAreSent_ReturnsActualData() {
        byte[] expected = new byte[] { 12, 83, 82 };
        doReturn(expected)
            .when(glueSchemaRegistryDeserializationFacade).getActualData(ENCODED_DATA);

        byte[] actual = glueSchemaRegistryDeserializer.getData(ENCODED_DATA);

        assertEquals(expected, actual);
    }

    @Test
    public void getSchema_WhenValidSchemaRegistryEncodedBytesAreSent_ReturnsSchema() {
        doReturn(SCHEMA_REGISTRY_SCHEMA)
            .when(glueSchemaRegistryDeserializationFacade).getSchema(ENCODED_DATA);

        Schema actual = glueSchemaRegistryDeserializer.getSchema(ENCODED_DATA);

        assertEquals(SCHEMA_REGISTRY_SCHEMA, actual);
    }

    @Test
    public void canDeserialize_WhenInvalidSchemaRegistryEncodedDataIsSent_ReturnsFalse() {
        assertFalse(glueSchemaRegistryDeserializer.canDeserialize(ENCODED_DATA));
    }

    @Test
    public void canDeserialize_WhenValidSchemaRegistryEncodedDataIsSent_ReturnsTrue() {
        byte[] encodedMessage = constructValidSerializedData();
        doReturn(true)
            .when(glueSchemaRegistryDeserializationFacade).canDeserialize(ENCODED_DATA);

        assertTrue(glueSchemaRegistryDeserializer.canDeserialize(ENCODED_DATA));
    }

    private byte[] constructValidSerializedData() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[18]);
        UUID uuid = UUID.randomUUID();
        byteBuffer.put(AWSSchemaRegistryConstants.HEADER_VERSION_BYTE);
        byteBuffer.put(AWSSchemaRegistryConstants.COMPRESSION_DEFAULT_BYTE);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        return byteBuffer.array();
    }
}