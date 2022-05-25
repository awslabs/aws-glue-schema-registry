/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.Map;

/**
 * JsonSchemaDataConfig - provides configuration options
 * Similar to AvroDataConfig
 */
public class ProtobufSchemaDataConfig extends AbstractConfig {
    public static final String CONNECT_META_DATA_CONFIG = "connect.meta.data";
    public static final boolean CONNECT_META_DATA_DEFAULT = true;
    public static final String CONNECT_META_DATA_DOC =
            "Toggle for enabling/disabling connect converter to add its meta data to the output schema " + "or not";

    public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.config";
    public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    public static final String SCHEMAS_CACHE_SIZE_DOC = "Size of the converted schemas cache";

    public ProtobufSchemaDataConfig(Map<?, ?> props) {
        super(baseConfigDef(), props);
    }

    public static ConfigDef baseConfigDef() {
        return new ConfigDef().define(CONNECT_META_DATA_CONFIG, ConfigDef.Type.BOOLEAN, CONNECT_META_DATA_DEFAULT,
                                      ConfigDef.Importance.LOW, CONNECT_META_DATA_DOC)
                .define(SCHEMAS_CACHE_SIZE_CONFIG, ConfigDef.Type.INT, SCHEMAS_CACHE_SIZE_DEFAULT,
                        ConfigDef.Importance.LOW, SCHEMAS_CACHE_SIZE_DOC);
    }

    public boolean isConnectMetaData() {
        return this.getBoolean(CONNECT_META_DATA_CONFIG);
    }

    public int getSchemasCacheSize() {
        return this.getInt(SCHEMAS_CACHE_SIZE_CONFIG);
    }

    public static class Builder {

        private Map<String, Object> props = new HashMap<>();

        public Builder with(String key,
                            Object value) {
            props.put(key, value);
            return this;
        }

        public ProtobufSchemaDataConfig build() {
            return new ProtobufSchemaDataConfig(props);
        }
    }
}
