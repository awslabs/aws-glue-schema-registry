
package com.amazonaws.services.schemaregistry.serializers.protobuf;

import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.ComplexNestingSyntax3;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import java.util.List;

/**
 * Generates Protobuf objects to be used during testing
 */
public class ProtobufGenerator {
    public static Basic.Address createCompiledProtobufRecord() {
        return Basic.Address.newBuilder()
                .setStreet("410 Terry Ave. North")
                .setCity("Seattle")
                .setZip(98109)
                .build();
    }

    public static DynamicMessage createDynamicProtobufRecord() {
        List<Descriptors.FieldDescriptor> fieldDescriptorList = Basic.Address.getDescriptor().getFields();
        return DynamicMessage.newBuilder(Basic.Address.getDescriptor())
                .setField(fieldDescriptorList.get(0), "5432 82nd St")
                .setField(fieldDescriptorList.get(1), 123456)
                .setField(fieldDescriptorList.get(2),"Seattle")
                .build();
    }

    public static DynamicMessage createDynamicNRecord() {
        return DynamicMessage.newBuilder(ComplexNestingSyntax3.N.getDescriptor())
                .setField(ComplexNestingSyntax3.N.getDescriptor().findFieldByName("A"), 100)
                .build();
    }
}