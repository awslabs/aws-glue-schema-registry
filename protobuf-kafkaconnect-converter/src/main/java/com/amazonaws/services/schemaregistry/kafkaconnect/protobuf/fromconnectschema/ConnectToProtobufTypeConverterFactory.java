package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_TYPE;

/**
 * Provides a converter instance that can convert the specific connect type to Protobuf type.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectToProtobufTypeConverterFactory {
    public static SchemaTypeConverter get(final Schema connectSchema) {
        final Schema.Type connectType = connectSchema.type();
        final Map<String, String> schemaParams = connectSchema.parameters();

        if (connectType.equals(Schema.Type.STRING)
                && schemaParams != null
                && schemaParams.containsKey(PROTOBUF_TYPE)
                && PROTOBUF_ENUM_TYPE.equals(schemaParams.get(PROTOBUF_TYPE))) {
            return new EnumSchemaTypeConverter();
        } else if (connectType.isPrimitive()) {
            return new PrimitiveSchemaTypeConverter();
        }

        throw new IllegalArgumentException("Unrecognized connect type: " + connectType);
    }
}
