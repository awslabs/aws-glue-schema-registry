package com.amazonaws.services.schemaregistry.deserializers.protobuf;

import com.google.protobuf.Descriptors;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ProtoParser;
import com.amazonaws.services.schemaregistry.utils.apicurio.FileDescriptorUtils;

/**
 * Utility class to parse the Protobuf schemas using square and apicurio library.
 */
public class ProtobufSchemaParser {
    public static Descriptors.FileDescriptor parse(final String schemaDefinition, final String protoFileName)
        throws Descriptors.DescriptorValidationException {
        ProtoFileElement fileElement = ProtoParser.Companion.parse(FileDescriptorUtils.DEFAULT_LOCATION, schemaDefinition);
        return FileDescriptorUtils.protoFileToFileDescriptor(fileElement, protoFileName);
    }
}
