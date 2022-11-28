package com.amazonaws.services.schemaregistry.serializers.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;
import java.math.BigDecimal;

public class JsonDoubleSerializer extends JsonSerializer<Double> {

    public JsonDoubleSerializer() {
        super();
    }

    @Override
    public void serialize(Double value, JsonGenerator jgen, SerializerProvider provider)
            throws IOException {
        System.out.println("value = " + value);
        jgen.writeNumber(new BigDecimal(value));
    }

}