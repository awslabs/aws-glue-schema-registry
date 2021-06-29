package com.amazonaws.services.schemaregistry.serializers.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;

@Builder
@AllArgsConstructor
@EqualsAndHashCode
@JsonSchemaInject(
        strings = {@JsonSchemaString(path = "className",
                value = "wrong.class.name")}
)
public class Employee {
    @JsonProperty
    private String name;

    public Employee() {}
}
