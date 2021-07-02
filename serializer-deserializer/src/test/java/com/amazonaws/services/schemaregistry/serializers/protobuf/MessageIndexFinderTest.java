package com.amazonaws.services.schemaregistry.serializers.protobuf;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.Basic;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax2.ComplexNestingSyntax2;
import com.amazonaws.services.schemaregistry.tests.protobuf.syntax3.ComplexNestingSyntax3;
import com.google.common.collect.BiMap;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MessageIndexFinderTest {

    private static final MessageIndexFinder messageIndexFinder = new MessageIndexFinder();

    @ParameterizedTest
    @MethodSource("testBasicProtoFileProvider")
    public void testGetAll_IdentifiesMessageIndex_ForBasicProtobufSchemas(ProtobufTestCase testCase) {
        Descriptors.FileDescriptor schema = testCase.getSchema();
        Map<String, Integer> expected = new HashMap<>();
        String packageName = testCase.getPackage();
        expected.put(packageName + ".Address", 0);
        expected.put(packageName + ".Customer", 1);

        Map<String, Integer> actual = getDescriptorNameMap(messageIndexFinder.getAll(schema.getFile()));

        assertEquals(expected, actual, testCase.getFileName());
    }

    @ParameterizedTest
    @MethodSource("testNestedProtoFileProvider")
    public void testGetAll_IdentifiesMessageIndex_ForNestedProtobufSchemas(ProtobufTestCase testCase) {
        Descriptors.FileDescriptor schema = testCase.getSchema();
        Map<String, Integer> expected = new HashMap<>();
        String packageName = testCase.getPackage();
        expected.put(packageName + ".A", 0);
        expected.put(packageName + ".A.B", 1);
        expected.put(packageName + ".A.B.C", 2);
        expected.put(packageName + ".A.B.C.J", 3);
        expected.put(packageName + ".A.B.C.J.K", 4);
        expected.put(packageName + ".A.B.C.X", 5);
        expected.put(packageName + ".A.B.C.X.D", 6);
        expected.put(packageName + ".A.B.C.X.D.F", 7);
        expected.put(packageName + ".A.B.C.X.D.F.M", 8);
        expected.put(packageName + ".A.B.C.X.D.G", 9);
        expected.put(packageName + ".A.B.C.X.L", 10);
        expected.put(packageName + ".A.I", 11);
        expected.put(packageName + ".A.X", 12);
        expected.put(packageName + ".N", 13);
        expected.put(packageName + ".O", 14);
        expected.put(packageName + ".O.A", 15);

        Map<String, Integer> actual = getDescriptorNameMap(messageIndexFinder.getAll(schema.getFile()));

        assertEquals(expected, actual, testCase.toString());
    }

    @ParameterizedTest
    @MethodSource("testDescriptorProvider")
    public void testGetByDescriptor_IdentifiesMessageIndex_ForGeneratedPOJO(Descriptors.Descriptor descriptor) {
        Integer actual = messageIndexFinder.getByDescriptor(descriptor.getFile(), descriptor);

        //A.B.C.X.D
        Integer expected = 6;

        assertEquals(expected, actual, descriptor.getFullName());
    }

    @ParameterizedTest
    @MethodSource("testMissingDescriptorProvider")
    public void testGetByDescriptor_WhenNonExistentTypePassed_ThrowsSchemaRegistryError(
        Descriptors.FileDescriptor fileDescriptor, Descriptors.Descriptor descriptor) {

        Exception ex = assertThrows(AWSSchemaRegistryException.class,
            () -> messageIndexFinder.getByDescriptor(fileDescriptor, descriptor));
        String errorMessage =
            String.format("Provided descriptor is not present in the schema: %s", descriptor.getFullName());

        assertEquals(errorMessage, ex.getMessage());

    }

    @ParameterizedTest
    @MethodSource("testBasicDescriptorFileProvider")
    public void testGetByDescriptor_WhenNullsArePassed_ThrowsValidationException(Descriptors.Descriptor descriptor) {

        assertThrows(IllegalArgumentException.class,
            () -> messageIndexFinder.getByDescriptor(descriptor.getFile(), null));

        assertThrows(IllegalArgumentException.class,
            () -> messageIndexFinder.getByDescriptor(null, descriptor));

    }

    @ParameterizedTest
    @MethodSource("testBasicDescriptorFileProvider")
    public void testGetByIndex_IdentifiesDescriptor_ForGeneratedPOJO(Descriptors.Descriptor descriptor) {
        Descriptors.Descriptor actual = messageIndexFinder.getByIndex(descriptor.getFile(), 1);

        //Basic.Customer
        Descriptors.Descriptor expected = Basic.Customer.getDescriptor();

        assertEquals(expected, actual, descriptor.getFullName());
    }

    @ParameterizedTest
    @MethodSource("testBasicDescriptorFileProvider")
    public void testGetByIndex_WhenNullsArePassed_ThrowsValidationException(Descriptors.Descriptor descriptor) {
        assertThrows(IllegalArgumentException.class,
            () -> messageIndexFinder.getByIndex(descriptor.getFile(), null));

        assertThrows(IllegalArgumentException.class,
            () -> messageIndexFinder.getByIndex(null, 1));
    }

    @ParameterizedTest
    @MethodSource("testMissingDescriptorProvider")
    public void testGetByIndex_WhenNonExistentIdPassed_ThrowsSchemaRegistryError(
        Descriptors.FileDescriptor fileDescriptor, Descriptors.Descriptor descriptor) {

        final Integer missingIndex = 100;
        Exception ex = assertThrows(AWSSchemaRegistryException.class,
            () -> messageIndexFinder.getByIndex(fileDescriptor, missingIndex));
        String errorMessage =
            String.format("No corresponding descriptor found for the index: %d", missingIndex);

        assertEquals(errorMessage, ex.getMessage());

    }

    private Map<String, Integer> getDescriptorNameMap(BiMap<Descriptors.Descriptor, Integer> descriptorMap) {
        return descriptorMap
            .entrySet()
            .stream()
            .collect(toMap(kv -> kv.getKey().getFullName(), Map.Entry::getValue));
    }

    private static List<Arguments> testBasicProtoFileProvider() {
        List<ProtobufTestCase> testCases =
            ProtobufTestCaseReader.getTestCasesByNames("Basic.proto");
        return testCases
            .stream()
            .map(Arguments::of)
            .collect(toList());
    }

    private static List<Arguments> testNestedProtoFileProvider() {
        List<ProtobufTestCase> testCases =
            ProtobufTestCaseReader.getTestCasesByNames("ComplexNestingSyntax3.proto", "ComplexNestingSyntax2.proto");
        return testCases
            .stream()
            .map(Arguments::of)
            .collect(toList());
    }

    private static List<Arguments> testBasicDescriptorFileProvider() {
        return Collections.singletonList(
            Arguments.of(
                Basic.Customer.getDescriptor()
            )
        );
    }

    private static List<Arguments> testDescriptorProvider() {
        return Arrays.asList(
            Arguments.of(
                ComplexNestingSyntax3.A.B.C.X.D.getDescriptor()
            ),
            Arguments.of(
                ComplexNestingSyntax2.A.B.C.X.D.getDescriptor()
            )
        );
    }

    private static List<Arguments> testMissingDescriptorProvider() {
        return Collections.singletonList(
            Arguments.of(
                //Search for Basic type Address in ComplexNesting.
                ComplexNestingSyntax3.getDescriptor().getFile(),
                Basic.Address.getDescriptor()
            )
        );
    }
}