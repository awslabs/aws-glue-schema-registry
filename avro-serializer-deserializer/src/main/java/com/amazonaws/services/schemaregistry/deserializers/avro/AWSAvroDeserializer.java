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

import com.amazonaws.services.schemaregistry.common.AWSCompressionFactory;
import com.amazonaws.services.schemaregistry.common.AWSDataFormatDeserializer;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.AWSDeserializerDataParser;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import com.amazonaws.services.schemaregistry.exception.AWSIncompatibleDataException;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Avro specific de-serializer responsible for handling the Avro protocol
 * specific conversion behavior.
 */
@Slf4j
public class AWSAvroDeserializer implements AWSDataFormatDeserializer {
    @Getter
    @Setter
    private GlueSchemaRegistryConfiguration schemaRegistrySerDeConfigs;
    @Setter
    private AvroRecordType avroRecordType;
    @Setter
    private AWSCompressionFactory compressionFactory;

    /**
     * Constructor accepting various dependencies.
     *
     * @param configs configuration elements
     */
    @Builder
    public AWSAvroDeserializer(GlueSchemaRegistryConfiguration configs) {
        this.schemaRegistrySerDeConfigs = configs;
        this.avroRecordType = configs.getAvroRecordType();

        compressionFactory = new AWSCompressionFactory();
    }

    /**
     * Deserialize the bytes to the original Avro message for the supplied schema.
     *
     * @param data   data to be de-serialized
     * @param schema Avro schema
     * @return de-serialized object
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    @Override
    public Object deserialize(@NonNull byte[] data, @NonNull String schema) {
        return deserialize(UUID.randomUUID(), ByteBuffer.wrap(data), schema);
    }

    /**
     * Deserialize the bytes to the original Avro message for the supplied schema.
     *
     * @param buffer data to be de-serialized
     * @param schema Avro schema
     * @return de-serialized object
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    @Override
    public Object deserialize(@NonNull ByteBuffer buffer, @NonNull String schema) {
        return deserialize(UUID.randomUUID(), buffer, schema);
    }

    /**
     * Deserialize the bytes to the original Avro message given the schema retrieved
     * from the schema registry.
     *
     * @param schemaVersionId schema version id for the Avro writer schema
     * @param buffer   data to be de-serialized
     * @param schema   Avro schema
     * @return de-serialized object
     * @throws AWSSchemaRegistryException Exception during de-serialization
     */
    @Override
    public Object deserialize(@NonNull UUID schemaVersionId, @NonNull ByteBuffer buffer, @NonNull String schema) {
        try {
            AWSDeserializerDataParser awsDeserializerDataParser = AWSDeserializerDataParser.getInstance();
            // Validate the data
            StringBuilder errorMessageBuilder = new StringBuilder();
            if (!awsDeserializerDataParser.isDataCompatible(buffer, errorMessageBuilder)) {
                throw new AWSIncompatibleDataException(errorMessageBuilder.toString());
            }

            byte[] data = awsDeserializerDataParser.getPlainData(buffer);

            log.debug("Length of actual message: {}, schema version id = {}", data.length, schemaVersionId);

            Schema schemaDefinition = getSchemaDefinition(schema);
            DatumReader<Object> datumReader = createDatumReader(schemaDefinition, schemaVersionId);
            BinaryDecoder binaryDecoder = getBinaryDecoder(data, 0, data.length);
            Object result = datumReader.read(null, binaryDecoder);

            log.debug("Finished de-serializing Avro message, schema version id: {}", schemaVersionId);

            return result;
        } catch (IOException | InstantiationException | IllegalAccessException e) {
            String message = String.format("Exception occurred while de-serializing Avro message, schema version id: %s",
                    schemaVersionId);
            throw new AWSSchemaRegistryException(message, e);
        }
    }

    private BinaryDecoder getBinaryDecoder(byte[] data, int start, int end) {
        return DecoderFactory.get().binaryDecoder(data, start, end, null);
    }

    private Schema getSchemaDefinition(String schema) {
        Schema schemaDefinition;
        try {
            schemaDefinition = (new Schema.Parser()).parse(schema);
        } catch (SchemaParseException e) {
            String message = "Error occurred while parsing schema, see inner exception for details. ";
            throw new AWSSchemaRegistryException(message, e);
        }
        return schemaDefinition;
    }

    /**
     * This method is used to create Avro datum reader for deserialization. By
     * default, it is GenericDatumReader; SpecificDatumReader will only be created
     * if the user specifies. In this case, the program will check if the user have
     * those specific code-generated schema class locally. ReaderSchema will be
     * supplied if the user wants to use a specific schema to deserialize the
     * message. (Compatibility check will be invoked)
     *
     * @param writerSchema schema that writes the Avro message
     * @param schemaVersionId     schema version id for the Avro writer schema
     * @return Avro datum reader for de-serialization
     * @throws InstantiationException can be thrown for readerClass.newInstance()
     *                                from java.lang.Class implementation
     * @throws IllegalAccessException can be thrown readerClass.newInstance() from
     *                                java.lang.Class implementation
     */
    public DatumReader<Object> createDatumReader(Schema writerSchema, UUID schemaVersionId)
            throws InstantiationException, IllegalAccessException {

        switch (this.avroRecordType) {
            case SPECIFIC_RECORD:
                @SuppressWarnings("unchecked")
                Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);

                Schema readerSchema = readerClass.newInstance().getSchema();
                log.debug("Using SpecificDatumReader for de-serializing Avro message, schema version id: {}, schema: {})",
                        schemaVersionId, readerSchema.toString());
                return new SpecificDatumReader<>(writerSchema, readerSchema);

            case GENERIC_RECORD:
                log.debug("Using GenericDatumReader for de-serializing Avro message, schema version id: {}, schema: {})",
                        schemaVersionId, writerSchema.toString());
                return new GenericDatumReader<>(writerSchema);

            default:
                String message = String.format("Data Format in configuration is not supported, Data Format: %s ",
                        this.avroRecordType.getName());
                throw new UnsupportedOperationException(message);
        }
    }
}
