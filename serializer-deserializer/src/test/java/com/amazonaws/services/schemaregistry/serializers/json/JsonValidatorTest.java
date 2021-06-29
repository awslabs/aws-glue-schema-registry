package com.amazonaws.services.schemaregistry.serializers.json;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.MissingNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonValidatorTest {
    private JsonValidator validator = new JsonValidator();
    private ObjectMapper mapper = new ObjectMapper();
    private String stringSchema = "{\n"
            + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
            + "  \"description\": \"String schema\",\n"
            + "  \"type\": \"string\"\n"
            + "}";

    @Test
    public void testBinaryNode() throws JsonProcessingException {
        byte[] bytes = "Test String".getBytes();
        JsonNode dataNode = new BinaryNode(bytes);
        assertEquals(dataNode.getNodeType(), JsonNodeType.BINARY);

        JsonNode schemaNode = mapper.readTree(stringSchema);
        validator.validateDataWithSchema(schemaNode, dataNode);
    }

    @Test
    public void testMissingNode() throws JsonProcessingException {
        JsonNode dataNode = MissingNode.getInstance();
        assertEquals(dataNode.getNodeType(), JsonNodeType.MISSING);

        JsonNode schemaNode = mapper.readTree(stringSchema);
        assertThrows(AWSSchemaRegistryException.class, ()-> {
            validator.validateDataWithSchema(schemaNode, dataNode);
        });
    }
}
