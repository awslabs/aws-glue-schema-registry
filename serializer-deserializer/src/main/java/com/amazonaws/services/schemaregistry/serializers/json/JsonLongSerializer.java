package com.amazonaws.services.schemaregistry.serializers.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.math.BigDecimal;

public class JsonLongSerializer extends JsonSerializer<Long> {

    public JsonLongSerializer() {
        super();
    }

    @Override
    public void serialize(Long value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException {
        jgen.writeNumber(new BigDecimal(value));
    }

}