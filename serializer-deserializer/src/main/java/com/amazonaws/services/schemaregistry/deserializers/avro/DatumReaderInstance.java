package com.amazonaws.services.schemaregistry.deserializers.avro;

import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

@Slf4j
public class DatumReaderInstance {
    private static final AVROUtils AVRO_UTILS = AVROUtils.getInstance();

    /**
     * This method is used to create Avro datum reader for deserialization. By
     * default, it is GenericDatumReader; SpecificDatumReader will only be created
     * if the user specifies. In this case, the program will check if the user have
     * those specific code-generated schema class locally. ReaderSchema will be
     * supplied if the user wants to use a specific schema to deserialize the
     * message. (Compatibility check will be invoked)
     *
     * @param writerSchemaDefinition Avro record writer schema.
     * @return Avro datum reader for de-serialization
     * @throws InstantiationException can be thrown for readerClass.newInstance()
     *                                from java.lang.Class implementation
     * @throws IllegalAccessException can be thrown readerClass.newInstance() from
     *                                java.lang.Class implementation
     */
    public static DatumReader<Object> from(String writerSchemaDefinition, AvroRecordType avroRecordType)
        throws InstantiationException, IllegalAccessException {

        Schema writerSchema = AVRO_UTILS.parseSchema(writerSchemaDefinition);

        switch (avroRecordType) {
            case SPECIFIC_RECORD:
                @SuppressWarnings("unchecked")
                Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);

                Schema readerSchema = readerClass.newInstance().getSchema();
                log.debug("Using SpecificDatumReader for de-serializing Avro message, schema: {})",
                    readerSchema.toString());
                return new SpecificDatumReader<>(writerSchema, readerSchema);

            case GENERIC_RECORD:
                log.debug("Using GenericDatumReader for de-serializing Avro message, schema: {})",
                    writerSchema.toString());
                return new GenericDatumReader<>(writerSchema);

            default:
                String message = String.format("Data Format in configuration is not supported, Data Format: %s ",
                    avroRecordType.getName());
                throw new UnsupportedOperationException(message);
        }
    }
}
