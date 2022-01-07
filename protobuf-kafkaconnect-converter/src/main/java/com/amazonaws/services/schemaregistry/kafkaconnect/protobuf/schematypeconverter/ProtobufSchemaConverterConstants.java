package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.schematypeconverter;

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
}
