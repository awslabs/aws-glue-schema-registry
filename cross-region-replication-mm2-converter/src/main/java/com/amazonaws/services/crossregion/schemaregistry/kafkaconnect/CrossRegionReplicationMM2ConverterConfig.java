package com.amazonaws.services.crossregion.schemaregistry.kafkaconnect;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class CrossRegionReplicationMM2ConverterConfig extends AbstractConfig {

    public static ConfigDef configDef() {
        return new ConfigDef();
    }
    /**
     * Constructor used by CrossRegionReplicationMM2Converter.
     * @param props property elements for the converter config
     */
    public CrossRegionReplicationMM2ConverterConfig(Map<String, ?> props) {
        super(configDef(), props);
    }
}
