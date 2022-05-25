package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isEnumType;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isTimeType;

/**
 * Provides a converter instance that can convert the specific connect type to Protobuf type.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectToProtobufTypeConverterFactory {
    public static SchemaTypeConverter get(final Schema connectSchema) {
        final Schema.Type connectType = connectSchema.type();

        if (isEnumType(connectSchema)) {
            return new EnumSchemaTypeConverter();
        } else if (isTimeType(connectSchema)) {
            return new TimeSchemaTypeConverter();
        } else if (Decimal.schema(DECIMAL_DEFAULT_SCALE).name().equals(connectSchema.name())) {
            return new DecimalSchemaTypeConverter();
        } else if (connectType.isPrimitive()) {
            return new PrimitiveSchemaTypeConverter();
        } else if (connectType.equals(Schema.Type.ARRAY)) {
            return new ArraySchemaTypeConverter();
        } else if (connectType.equals(Schema.Type.MAP)) {
            return new MapSchemaTypeConverter();
        } else if (connectType.equals(Schema.Type.STRUCT)) {
            return new StructSchemaTypeConverter();
        }

        throw new IllegalArgumentException("Unrecognized connect type: " + connectType);
    }
}
