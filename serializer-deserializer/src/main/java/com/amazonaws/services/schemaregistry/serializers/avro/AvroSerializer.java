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
package com.amazonaws.services.schemaregistry.serializers.avro;

import com.amazonaws.services.schemaregistry.common.GlueSchemaRegistryDataFormatSerializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;

/**
 * Avro serialization helper.
 */
@Slf4j
public class AvroSerializer implements GlueSchemaRegistryDataFormatSerializer {
    private AVROUtils avroUtils = AVROUtils.getInstance();
    private static final long MAX_DATUM_WRITER_CACHE_SIZE = 100;

    @NonNull
    @VisibleForTesting
    protected final LoadingCache<DatumWriterCacheKey, DatumWriter<Object>> datumWriterCache;

    public AvroSerializer() {
        this.datumWriterCache =
            CacheBuilder
                .newBuilder()
                .maximumSize(MAX_DATUM_WRITER_CACHE_SIZE)
                .build(new DatumWriterCache());
    }

    @Override
    public byte[] serialize(Object data) {
        byte[] bytes;
        bytes = serialize(data, createDatumWriter(data));

        return bytes;
    }

    /**
     * This method is used to create Avro datum writer for serialization. Based on
     * the Avro record type, GenericDatumWriter or SpecificDatumWriter will be
     * created.
     *
     * @param object the Avro message
     * @return Avro datum writer for serialization
     */
    private DatumWriter<Object> createDatumWriter(Object object) {
        org.apache.avro.Schema schema = AVROUtils.getInstance()
                .getSchema(object);
        if (object instanceof SpecificRecord) {
            return getSpecificDatumWriter(schema);
        } else if (object instanceof GenericRecord) {
            return getGenericDatumWriter(schema);
        } else if (object instanceof GenericData.EnumSymbol) {
            return getGenericDatumWriter(schema);
        } else if (object instanceof GenericData.Array) {
            return getGenericDatumWriter(schema);
        } else if (object instanceof GenericData.Fixed) {
            return getGenericDatumWriter(schema);
        } else {
            String message =
                String.format("Unsupported type passed for serialization: %s", object);
            throw new AWSSchemaRegistryException(message);
        }
    }

    @SneakyThrows
    private DatumWriter<Object> getSpecificDatumWriter(Schema schema) {
        DatumWriterCacheKey datumWriterCacheKey = new DatumWriterCacheKey(schema, AvroRecordType.SPECIFIC_RECORD);
        return datumWriterCache.get(datumWriterCacheKey);
    }

    @SneakyThrows
    private DatumWriter<Object> getGenericDatumWriter(Schema schema) {
        DatumWriterCacheKey datumWriterCacheKey = new DatumWriterCacheKey(schema, AvroRecordType.GENERIC_RECORD);
        return datumWriterCache.get(datumWriterCacheKey);
    }

    /**
     * Serialize the Avro message to bytes
     *
     * @param data   the Avro message for serialization
     * @param writer Avro DatumWriter for serialization
     * @return the serialized byte array
     * @throws AWSSchemaRegistryException AWS Schema Registry Exception
     */
    private byte[] serialize(Object data, DatumWriter<Object> writer) {
        return encodeData(data, writer);
    }

    private byte[] encodeData(Object object, DatumWriter<Object> writer) {
        ByteArrayOutputStream actualDataBytes = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(actualDataBytes, null);
        try {
            writer.write(object, encoder);
            encoder.flush();
        } catch (Exception e) {
            throw new AWSSchemaRegistryException(e.getMessage(), e);
        }
        return actualDataBytes.toByteArray();
    }

    /**
     * Get the schema definition.
     *
     * @param object object for which schema definition has to be derived
     * @return schema string
     */
    @Override
    public String getSchemaDefinition(@NonNull Object object) {
        return avroUtils.getSchemaDefinition(object);
    }

    public void validate(Object data) {
        //No-op
        //Avro format assumes that the passed object contains schema and data that are mutually conformant.
        //We cannot validate the data against the schema.
    }

    public void validate(String schemaDefinition, byte[] data) {
        //No-op
        //We cannot determine accurately if the data bytes match the schema as Avro bytes don't contain the field names.
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    private static class DatumWriterCacheKey {
        @NonNull
        private final Schema schema;
        @NonNull
        private final AvroRecordType avroRecordType;
    }

    private static class DatumWriterCache extends CacheLoader<DatumWriterCacheKey, DatumWriter<Object>> {
        @Override
        public DatumWriter<Object> load(DatumWriterCacheKey datumWriterCacheKey) {
            Schema schema = datumWriterCacheKey.getSchema();
            AvroRecordType avroRecordType = datumWriterCacheKey.getAvroRecordType();
            return DatumWriterInstance.get(schema, avroRecordType);
        }
    }
}
