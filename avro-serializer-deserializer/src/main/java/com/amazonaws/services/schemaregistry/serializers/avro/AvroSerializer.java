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

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;

/**
 * Avro serialization helper.
 */
@Slf4j
public class AvroSerializer {
    private final org.apache.avro.Schema schema;

    public AvroSerializer(org.apache.avro.Schema schema) {
        this.schema = schema;
    }

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
        if (object instanceof SpecificRecord) {
            return new SpecificDatumWriter<>(schema);
        } else if (object instanceof GenericRecord) {
            return new GenericDatumWriter<>(schema);
        } else if (object instanceof GenericData.EnumSymbol) {
            return new GenericDatumWriter<>(schema);
        } else if (object instanceof GenericData.Array) {
            return new GenericDatumWriter<>(schema);
        } else if (object instanceof GenericData.Fixed) {
            return new GenericDatumWriter<>(schema);
        } else {
            String message =
                String.format("Unsupported type passed for serialization: %s", object);
            throw new AWSSchemaRegistryException(message);
        }
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
}
