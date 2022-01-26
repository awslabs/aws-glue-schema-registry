package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf;

import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata.ConnectDataToProtobufDataConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ConnectSchemaToProtobufSchemaConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectdata.ProtobufDataToConnectDataConverter;
import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.toconnectschema.ProtobufSchemaToConnectSchemaConverter;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ProtobufSchemaConverter implements Converter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtobufSchemaConverter.class);

    private final GlueSchemaRegistryKafkaSerializer serializer;
    private final GlueSchemaRegistryKafkaDeserializer deserializer;

    private ConnectSchemaToProtobufSchemaConverter connectSchemaToProtobufSchemaConverter;
    private ConnectDataToProtobufDataConverter connectDataToProtobufDataConverter;
    private ProtobufSchemaToConnectSchemaConverter protobufSchemaToConnectSchemaConverter;
    private ProtobufDataToConnectDataConverter protobufDataToConnectDataConverter;

    private boolean isKey;

    //Used for testing
    public ProtobufSchemaConverter(
        final GlueSchemaRegistryKafkaSerializer serializer,
        final GlueSchemaRegistryKafkaDeserializer deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public ProtobufSchemaConverter() {
        this.serializer = new GlueSchemaRegistryKafkaSerializer();
        this.serializer.setUserAgentApp(UserAgents.KAFKACONNECT);

        this.deserializer = new GlueSchemaRegistryKafkaDeserializer();
        this.deserializer.setUserAgentApp(UserAgents.KAFKACONNECT);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        //Add Config here.
        this.serializer.configure(configs, this.isKey);
        this.deserializer.configure(configs, this.isKey);
        this.connectSchemaToProtobufSchemaConverter = new ConnectSchemaToProtobufSchemaConverter();
        this.connectDataToProtobufDataConverter = new ConnectDataToProtobufDataConverter();
        this.protobufSchemaToConnectSchemaConverter = new ProtobufSchemaToConnectSchemaConverter();
        this.protobufDataToConnectDataConverter = new ProtobufDataToConnectDataConverter();
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        final Descriptors.FileDescriptor fileDescriptor = connectSchemaToProtobufSchemaConverter.convert(schema);
        final Message message = connectDataToProtobufDataConverter.convert(fileDescriptor, schema, value);

        return serializer.serialize(topic, message);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        final Message message = (Message) deserializer.deserialize(topic, bytes);
        final Schema schema = protobufSchemaToConnectSchemaConverter.toConnectSchema(message);
        final Object value = protobufDataToConnectDataConverter.toConnectData(message, schema);

        return new SchemaAndValue(schema, value);
    }
}
