package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectDataToProtobufDataConverterFactory {

    public static DataConverter get(final Schema connectSchema) {
        final Schema.Type connectType = connectSchema.type();
        final Map<String, String> schemaParams = connectSchema.parameters();

        if (Schema.Type.STRING.equals(connectType)
                && schemaParams != null
                && schemaParams.containsKey(PROTOBUF_TYPE)
                && "enum".equals(schemaParams.get(PROTOBUF_TYPE))) {
            return new EnumDataConverter();
        } else if (connectType.isPrimitive()) {
            return new PrimitiveDataConverter();
        } else if (connectType.equals(Schema.Type.ARRAY)) {
            return new ArrayDataConverter();
        }

        throw new IllegalArgumentException("Unrecognized connect type: " + connectType);
    }
}
