package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf;

import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
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

    private ConnectSchemaToProtobufSchemaConverter connectSchemaToProtobufConverter;

    private boolean isKey;

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
        this.connectSchemaToProtobufConverter = new ConnectSchemaToProtobufSchemaConverter();
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        Descriptors.FileDescriptor fileDescriptor = connectSchemaToProtobufConverter.convert(schema);
        //TBD: Implement this
//      Message message = connectValueToProtobufDataConverter.convert(schema, value);
        Message message = null;

        return serializer.serialize(topic, message);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] bytes) {
        return null;
    }
}
