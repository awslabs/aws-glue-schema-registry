package com.amazonaws.services.schemaregistry.serializers.avro;

import com.amazonaws.services.schemaregistry.utils.RecordGenerator;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroSerializerTest {

    @Test
    public void serialize_WhenSerializeIsCalled_ReturnsCachedInstance() {
        AvroSerializer avroSerializer = new AvroSerializer();

        User specificUserRecord = RecordGenerator.createSpecificAvroRecord();
        GenericRecord genericUserRecord = RecordGenerator.createGenericUserMapAvroRecord();

        avroSerializer.serialize(specificUserRecord);
        avroSerializer.serialize(genericUserRecord);
        //Same schema won't be cached again.
        avroSerializer.serialize(genericUserRecord);

        assertEquals(2, avroSerializer.datumWriterCache.size());
    }
}