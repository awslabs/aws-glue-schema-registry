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
package com.amazonaws.services.schemaregistry.deserializers.avro;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerDataParser;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.nio.ByteBuffer;

/**
 * Avro specific de-serializer responsible for handling the Avro protocol
 * specific conversion behavior.
 */
@Slf4j
public class AvroDeserializer implements GlueSchemaRegistryDataFormatDeserializer {
    private static final GlueSchemaRegistryDeserializerDataParser DESERIALIZER_DATA_PARSER =
        GlueSchemaRegistryDeserializerDataParser.getInstance();
    //TODO: Make this configurable if requested by customers.
    private static final long MAX_DATUM_READER_CACHE_SIZE = 100;

    @Getter
    @Setter
    private GlueSchemaRegistryConfiguration schemaRegistrySerDeConfigs;
    @Setter
    private AvroRecordType avroRecordType;

    @Setter
    private boolean logicalTypesConversionEnabled;

    @NonNull
    @Getter
    @VisibleForTesting
    protected final LoadingCache<String, DatumReader<Object>> datumReaderCache;

    /**
     * Constructor accepting various dependencies.
     *
     * @param configs configuration elements
     */
    @Builder
    public AvroDeserializer(GlueSchemaRegistryConfiguration configs) {
        this.schemaRegistrySerDeConfigs = configs;
        this.avroRecordType = configs.getAvroRecordType();
        this.logicalTypesConversionEnabled = configs.isLogicalTypesConversionEnabled();
        this.datumReaderCache =
            CacheBuilder
                .newBuilder()
                .maximumSize(MAX_DATUM_READER_CACHE_SIZE)
                .build(new DatumReaderCache());
    }

    /**
     * Deserialize the bytes to the original Avro message given the schema retrieved
     * from the schema registry.
     *
     * @param buffer   data to be de-serialized
     * @param schemaObject  Avro schema
     * @return de-serialized object
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    @Override
    public Object deserialize(@NonNull ByteBuffer buffer,
        @NonNull com.amazonaws.services.schemaregistry.common.Schema schemaObject) {
        try {
            String schema = schemaObject.getSchemaDefinition();
            byte[] data = DESERIALIZER_DATA_PARSER.getPlainData(buffer);

            log.debug("Length of actual message: {}", data.length);

            DatumReader<Object> datumReader = datumReaderCache.get(schema);

            BinaryDecoder binaryDecoder = getBinaryDecoder(data, 0, data.length);
            Object result = datumReader.read(null, binaryDecoder);

            log.debug("Finished de-serializing Avro message");

            return result;
        } catch (Exception e) {
            String message = "Exception occurred while de-serializing Avro message";
            throw new AWSSchemaRegistryException(message, e);
        }
    }

    private BinaryDecoder getBinaryDecoder(byte[] data, int start, int end) {
        return DecoderFactory.get().binaryDecoder(data, start, end, null);
    }

    private class DatumReaderCache extends CacheLoader<String, DatumReader<Object>> {
        @Override
        public DatumReader<Object> load(String schema) throws Exception {
            return DatumReaderInstance.from(schema, avroRecordType, logicalTypesConversionEnabled);
        }
    }
}
