package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ProtobufSchemaConverterConstants {
    /**
     * Kafka Converters / connectors can define this property in Connect schema
     * to specify the Protobuf tag number to use for a field while generating the Protobuf schema.
     */
    public static final String PROTOBUF_TAG = "protobuf.tag";

    /**
     * Kafka Converters / connectors can define this property in Connect schema
     * to specify the Protobuf type to use for a field while generating the Protobuf schema.
     * Ex: int32 can be mapped to sint32, uint32 etc. It will be default to int32 if not specified.
     */
    public static final String PROTOBUF_TYPE = "protobuf.type";

    /**
     * Specifies the package name of Protobuf schema definition.
     * This will be available in the parent level connect schema parameters.
     */
    public static final String PROTOBUF_PACKAGE = "protobuf.package";

    /**
     * Used to specify the metadata parameter containing the name of the enum
     */
    public static final String PROTOBUF_ENUM_NAME = "ENUM_NAME";

    /**
     * Used to mark metadata parameters containing values for the enum
     */
    public static final String PROTOBUF_ENUM_VALUE = "PROTOBUF_ENUM_VALUE.";

    /**
     * The string used to validate that protobuf type is enum
     */
    public static final String PROTOBUF_ENUM_TYPE = "enum";

    /**
     * The string used to validate that protobuf type is oneof
     */
    public static final String PROTOBUF_ONEOF_TYPE = "oneof";
  
    /**
     * Kafka Connect's Decimal builder requires a default scale: https://kafka.apache.org/0100/javadoc/org/apache/kafka/connect/data/Decimal.html#builder(int)
     * Our converter overrides this value during the conversion, so we enter this value just as a temporary default during the creation.
     */
    public static final int DECIMAL_DEFAULT_SCALE = 0;
}
