package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.schematypeconverter;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;

/**
 * Provides a converter instance that can convert the specific connect type to Protobuf type.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectToProtobufTypeConverterFactory {
    public static SchemaTypeConverter get(final Schema connectSchema) {
        final Schema.Type connectType = connectSchema.type();

        if (connectType.isPrimitive()) {
            return new PrimitiveSchemaTypeConverter();
        }

        throw new IllegalArgumentException("Unrecognized connect type: " + connectType);
    }
}
