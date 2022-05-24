package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Decimal;

import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectDataToProtobufDataConverterFactory {
    public static DataConverter get(final Schema connectSchema) {
        final Schema.Type connectType = connectSchema.type();
        final Map<String, String> schemaParams = connectSchema.parameters();

        if (Schema.Type.STRING.equals(connectType)
                && schemaParams != null
                && schemaParams.containsKey(PROTOBUF_TYPE)
                && PROTOBUF_ENUM_TYPE.equals(schemaParams.get(PROTOBUF_TYPE))) {
            return new EnumDataConverter();
        } else if (Date.SCHEMA.name().equals(connectSchema.name())
                || Timestamp.SCHEMA.name().equals(connectSchema.name())
                || Time.SCHEMA.name().equals(connectSchema.name())) {
            return new TimeDataConverter();
        } else if (Decimal.schema(0).name().equals(connectSchema.name())) {
            return new DecimalDataConverter();
        } else if (connectType.isPrimitive()) {
            return new PrimitiveDataConverter();
        } else if (connectType.equals(Schema.Type.ARRAY)) {
            return new ArrayDataConverter();
        }

        throw new IllegalArgumentException("Unrecognized connect type: " + connectType);
    }
}
