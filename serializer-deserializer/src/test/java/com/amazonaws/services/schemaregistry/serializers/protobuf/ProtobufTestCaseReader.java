package com.amazonaws.services.schemaregistry.serializers.protobuf;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ProtobufTestCaseReader {
    public static final String TEST_METADATA_PATH = "src/test/resources/protobuf/";
    public static final String TEST_PROTO_PATH = "src/test/proto/";

    public static List<ProtobufTestCase> getTestCases() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(
                Paths.get(TEST_METADATA_PATH + "TestMetadata.json").toFile(),
                new TypeReference<List<ProtobufTestCase>>() { }
            );
        } catch (IOException e) {
            throw new RuntimeException("Error parsing test metadata JSON file", e);
        }
    }

    public static List<ProtobufTestCase> getTestCasesByNames(String ... names) {
        List<ProtobufTestCase> testCases = getTestCases();
        return Arrays.stream(names)
            .map(ProtobufTestCaseReader::getTestCaseByName)
            .collect(toList());
    }

    public static ProtobufTestCase getTestCaseByName(String name) {
        List<ProtobufTestCase> testCases = getTestCases();
        return testCases
            .stream()
            .filter(
                testCase -> testCase.getFileName().equals(name)
            )
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Test case file not found: " + name));
    }
}
