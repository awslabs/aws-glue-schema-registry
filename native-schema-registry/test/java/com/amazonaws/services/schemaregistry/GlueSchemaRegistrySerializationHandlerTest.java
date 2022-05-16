package com.amazonaws.services.schemaregistry;

import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.model.DataFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

class GlueSchemaRegistrySerializationHandlerTest {
    private static final String TRANSPORT_NAME = "some-transport-name";
    private static final String SCHEMA_NAME = "some-schema-name";
    private static final String SCHEMA_DEF = "package b; message B {}";
    private static final String DATA_FORMAT = DataFormat.PROTOBUF.name();

    private static final byte[] BYTES_TO_ENCODE = new byte[] {
        90, 112, 100, 89, 45, 87
    };

    private static final byte[] ENCODED_BYTES = new byte[] {
        3, 1, 0, 12, 16, 90, 112, 100, 89, 45, 87
    };

    @Mock
    private IsolateThread isolateThread;

    @Mock
    private GlueSchemaRegistrySerializer mockGlueSchemaRegistrySerializer;

    @Test
    void initializeSerializer_initializes_a_serializer_instance() {

        //Assert that SerializerInstance is in un-initialized state.
        Exception exception = assertThrows(IllegalStateException.class, SerializerInstance::get);
        assertEquals("Serializer is not initialized.", exception.getMessage());

        GlueSchemaRegistryConfiguration expectedConfiguration = new GlueSchemaRegistryConfiguration("us-east-1");
        expectedConfiguration.setSchemaAutoRegistrationEnabled(true);

        //Verify config is passed as it is.
        try (MockedStatic<SerializerInstance> serializerInstanceMock = Mockito.mockStatic(SerializerInstance.class)) {
            serializerInstanceMock
                .when(() -> SerializerInstance.create(expectedConfiguration))
                .thenCallRealMethod();

            GlueSchemaRegistrySerializationHandler.initializeSerializer(isolateThread);
            serializerInstanceMock.verify(() -> SerializerInstance.create(expectedConfiguration));
        }

        //Verify actual initialization happens.
        assertNotNull(SerializerInstance.get());
    }

//TODO: This test outside "native-image" build i.e. in a JVM. We need a way to unit-test this logic.
//Possibly, write C tests.
//    @Test
//    void encodeWithSchema_encodesDataWithSchema_ReturnsTheEncodedBytes() {
//        CCharPointer transportName = CTypeConversion.toCString(TRANSPORT_NAME).get();
////        CCharPointer transportName = mock(CCharPointer.class);
//
//        Schema gsrSchema = new Schema(SCHEMA_NAME, SCHEMA_DEF, DATA_FORMAT);
//        GlueSchemaRegistrySerializationHandler.C_GlueSchemaRegistrySchema c_glueSchemaRegistrySchema =
//            MockedCStructures.mockCGlueSchemaRegistrySchema(SCHEMA_NAME, SCHEMA_DEF, DATA_FORMAT);
//
//        GlueSchemaRegistrySerializationHandler.C_ReadOnlyByteArray c_readOnlyByteArray =
//            MockedCStructures.mockCReadOnlyByteArray(BYTES_TO_ENCODE, BYTES_TO_ENCODE.length);
//
//        GlueSchemaRegistrySerializationHandler.C_MutableByteArray expected =
//            MockedCStructures.mockCMutableByteArray(ENCODED_BYTES, ENCODED_BYTES.length);
//
//        try (MockedStatic<SerializerInstance> serializerInstanceMock = Mockito.mockStatic(SerializerInstance.class)) {
//            serializerInstanceMock
//                .when(SerializerInstance::get)
//                .thenReturn(mockGlueSchemaRegistrySerializer);
//
//            doReturn(ENCODED_BYTES).when(mockGlueSchemaRegistrySerializer)
//                .encode(TRANSPORT_NAME, gsrSchema, BYTES_TO_ENCODE);
//
//            GlueSchemaRegistrySerializationHandler.C_MutableByteArray actual = GlueSchemaRegistrySerializationHandler
//                .encodeWithSchema(isolateThread, transportName, c_readOnlyByteArray, c_glueSchemaRegistrySchema);
//
//            assertEquals(expected, actual);
//            serializerInstanceMock.verify(SerializerInstance::get);
//        }
//    }
}