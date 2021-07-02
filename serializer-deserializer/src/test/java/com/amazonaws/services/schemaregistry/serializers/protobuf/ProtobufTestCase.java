package com.amazonaws.services.schemaregistry.serializers.protobuf;

import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import io.apicurio.registry.utils.protobuf.schema.FileDescriptorUtils;
import lombok.Data;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

@Data
public class ProtobufTestCase {
    private String fileName;

    public String getRawSchema() {
        try {
            return new String(Files.readAllBytes(Paths.get(ProtobufTestCaseReader.TEST_PROTO_PATH, fileName)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Error reading file", e);
        }
    }

    public String getPackage() {
        return getSchema().getPackage();
    }

    public Descriptors.FileDescriptor getSchema() {
        String rawSchema = this.getRawSchema();
        ProtoFileElement fileElem = ProtoParser.Companion.parse(FileDescriptorUtils.DEFAULT_LOCATION, rawSchema);
        Descriptors.FileDescriptor fileDescriptor = null;
        try {
            fileDescriptor = FileDescriptorUtils.protoFileToFileDescriptor(fileElem);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new RuntimeException("Error parsing descriptors from Protobuf schema", e);
        }
        return fileDescriptor;
    }

}
