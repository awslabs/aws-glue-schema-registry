package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isEnumType;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isTimeType;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectDataToProtobufDataConverterFactory {
    public static DataConverter get(final Schema connectSchema) {
        final Schema.Type connectType = connectSchema.type();

        if (isEnumType(connectSchema)) {
            return new EnumDataConverter();
        } else if (isTimeType(connectSchema)) {
            return new TimeDataConverter();
        } else if (Decimal.schema(DECIMAL_DEFAULT_SCALE).name().equals(connectSchema.name())) {
            return new DecimalDataConverter();
        } else if (connectType.isPrimitive()) {
            return new PrimitiveDataConverter();
        } else if (connectType.equals(Schema.Type.ARRAY)) {
            return new ArrayDataConverter();
        }

        throw new IllegalArgumentException("Unrecognized connect type: " + connectType);
    }
}
