package com.amazonaws.services.schemaregistry.serializers.avro;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class DatumWriterInstance {
    public static DatumWriter<Object> get(Schema schema, AvroRecordType avroRecordType) {
        switch (avroRecordType) {
            case SPECIFIC_RECORD:
                return new SpecificDatumWriter<>(schema);
            case GENERIC_RECORD:
                return new GenericDatumWriter<>(schema);
            case UNKNOWN:
            default:
                String message =
                    String.format("Unsupported type passed for serialization: %s", avroRecordType);
                throw new AWSSchemaRegistryException(message);
        }
    }
}
