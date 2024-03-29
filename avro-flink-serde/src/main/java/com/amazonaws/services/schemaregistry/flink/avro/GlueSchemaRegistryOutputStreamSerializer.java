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

package com.amazonaws.services.schemaregistry.flink.avro;

import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.common.configs.UserAgents;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.GlueSchemaRegistryUtils;
import org.apache.avro.Schema;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * AWS Glue Schema Registry output stream serializer to accept schema and output stream to register schema
 * and write serialized object with schema registry bytes to output stream.
 */
public class GlueSchemaRegistryOutputStreamSerializer {
    private final String transportName;
    private final Map<String, Object> configs;
    private final GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade;

    public GlueSchemaRegistryOutputStreamSerializer(String transportName, Map<String, Object> configs) {
        this(transportName, configs, null);
    }

    public GlueSchemaRegistryOutputStreamSerializer(String transportName, Map<String, Object> configs,
        GlueSchemaRegistrySerializationFacade glueSchemaRegistrySerializationFacade) {
        this.transportName = transportName;
        this.configs = configs;
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        glueSchemaRegistryConfiguration.setUserAgentApp(UserAgents.FLINK);
        this.glueSchemaRegistrySerializationFacade = glueSchemaRegistrySerializationFacade != null
            ? glueSchemaRegistrySerializationFacade
            : GlueSchemaRegistrySerializationFacade
                .builder()
                .credentialProvider(DefaultCredentialsProvider.builder().build())
                .glueSchemaRegistryConfiguration(new GlueSchemaRegistryConfiguration(configs))
                .build();
    }

    /**
     * Register schema and write serialized object with schema registry bytes to output stream
     * @param schema schema to be registered
     * @param out    output stream
     * @param data   original bytes of serialized object
     * @throws IOException
     */
    public void registerSchemaAndSerializeStream(Schema schema, OutputStream out, byte[] data) throws IOException {
        byte[] bytes = glueSchemaRegistrySerializationFacade.encode(
                transportName,
                new com.amazonaws.services.schemaregistry.common.Schema(schema.toString(), DataFormat.AVRO.toString(), getSchemaName()),
                data);
        out.write(bytes);
    }

    private String getSchemaName() {
        String schemaName = GlueSchemaRegistryUtils.getInstance().getSchemaName(configs);

        return schemaName != null
                ? schemaName
                : GlueSchemaRegistryUtils.getInstance().configureSchemaNamingStrategy(configs).getSchemaName(transportName);
    }
}
