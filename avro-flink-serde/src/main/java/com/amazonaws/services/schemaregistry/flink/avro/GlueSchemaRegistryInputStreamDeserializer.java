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
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;


/**
 * AWS Glue Schema Registry input stream de-serializer to accept input stream and extract schema from it
 * and remove schema registry information in the input stream.
 */
public class GlueSchemaRegistryInputStreamDeserializer {
    private final GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade;

    /**
     * Constructor accepts configuration map for AWS Deserializer
     *
     * @param configs configuration map
     */
    public GlueSchemaRegistryInputStreamDeserializer(Map<String, Object> configs) {
        GlueSchemaRegistryConfiguration glueSchemaRegistryConfiguration = new GlueSchemaRegistryConfiguration(configs);
        glueSchemaRegistryConfiguration.setUserAgentApp(UserAgents.FLINK);
        glueSchemaRegistryDeserializationFacade =
            new GlueSchemaRegistryDeserializationFacade(glueSchemaRegistryConfiguration, DefaultCredentialsProvider.builder().build());
    }

    public GlueSchemaRegistryInputStreamDeserializer(GlueSchemaRegistryDeserializationFacade glueSchemaRegistryDeserializationFacade) {
        this.glueSchemaRegistryDeserializationFacade = glueSchemaRegistryDeserializationFacade;
    }

    /**
     * Get schema and remove extra Schema Registry information within input stream
     *
     * @param in input stream
     * @return schema of object within input stream
     * @throws IOException Exception during decompression
     */
    public Schema getSchemaAndDeserializedStream(InputStream in) throws IOException {
        byte[] inputBytes = new byte[in.available()];
        in.read(inputBytes);
        in.reset();

        MutableByteArrayInputStream mutableByteArrayInputStream = (MutableByteArrayInputStream) in;
        String schemaDefinition = glueSchemaRegistryDeserializationFacade.getSchema(inputBytes).getSchemaDefinition();
        byte[] deserializedBytes = glueSchemaRegistryDeserializationFacade.getActualData(inputBytes);
        mutableByteArrayInputStream.setBuffer(deserializedBytes);

        Schema schema;
        try {
            schema = (new Schema.Parser()).parse(schemaDefinition);
        } catch (SchemaParseException e) {
            String message = "Error occurred while parsing schema, see inner exception for details.";
            throw new AWSSchemaRegistryException(message, e);
        }

        return schema;
    }
}