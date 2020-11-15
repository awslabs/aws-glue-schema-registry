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

import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.flink.formats.avro.SchemaCoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * Schema coder that allows reading schema that is somehow embedded into serialized record.
 * Used by {@link GlueSchemaRegistryAvroDeserializationSchema} and {@link GlueSchemaRegistryAvroSerializationSchema}.
 */
public class GlueSchemaRegistryAvroSchemaCoder implements SchemaCoder {
    private GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer;
    private GlueSchemaRegistryOutputStreamSerializer glueSchemaRegistryOutputStreamSerializer;

    /**
     * Constructor accepts transport name and configuration map for AWS Glue Schema Registry
     *
     * @param transportName topic name or stream name etc.
     * @param configs       configurations for AWS Glue Schema Registry
     */
    public GlueSchemaRegistryAvroSchemaCoder(final String transportName, final Map<String, Object> configs) {
        glueSchemaRegistryInputStreamDeserializer = new GlueSchemaRegistryInputStreamDeserializer(configs);
        glueSchemaRegistryOutputStreamSerializer = new GlueSchemaRegistryOutputStreamSerializer(transportName, configs);
    }

    @VisibleForTesting
    protected GlueSchemaRegistryAvroSchemaCoder(final GlueSchemaRegistryInputStreamDeserializer glueSchemaRegistryInputStreamDeserializer) {
        this.glueSchemaRegistryInputStreamDeserializer = glueSchemaRegistryInputStreamDeserializer;
    }

    @VisibleForTesting
    protected GlueSchemaRegistryAvroSchemaCoder(final GlueSchemaRegistryOutputStreamSerializer glueSchemaRegistryOutputStreamSerializer) {
        this.glueSchemaRegistryOutputStreamSerializer = glueSchemaRegistryOutputStreamSerializer;
    }

    @Override
    public Schema readSchema(InputStream in) throws IOException {
        return glueSchemaRegistryInputStreamDeserializer.getSchemaAndDeserializedStream(in);
    }

    @Override
    public void writeSchema(Schema schema, OutputStream out) throws IOException {
        byte[] data = ((ByteArrayOutputStream) out).toByteArray();
        ((ByteArrayOutputStream) out).reset();
        glueSchemaRegistryOutputStreamSerializer.registerSchemaAndSerializeStream(schema, out, data);
    }
}