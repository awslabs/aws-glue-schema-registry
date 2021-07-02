package com.amazonaws.services.schemaregistry.serializers.protobuf;

import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.Descriptors;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static java.util.Comparator.comparing;

/**
 * MessageIndexFinder finds the position of message type in the overall schema.
 * This position is used to serialize / deserialize the correct Message type.
 */
public class MessageIndexFinder {

    /**
     * Parses the given Schema descriptor, assigns indices and finds the index for the given descriptor.
     *
     * @param schemaDescriptor Protobuf Schema Descriptor
     * @param descriptorToFind Protobuf Descriptor to find.
     * @return Index Integer.
     */
    public Integer getByDescriptor(
        @NonNull Descriptors.FileDescriptor schemaDescriptor,
        @NonNull Descriptors.Descriptor descriptorToFind) {

        final BiMap<Descriptors.Descriptor, Integer> descriptorToIndexBiMap = getAll(schemaDescriptor);

        if (!descriptorToIndexBiMap.containsKey(descriptorToFind)) {
            final String errorMessage = String.format(
                "Provided descriptor is not present in the schema: %s", descriptorToFind.getFullName());
            throw new AWSSchemaRegistryException(errorMessage);
        }

        return descriptorToIndexBiMap.get(descriptorToFind);
    }

    /**
     * Parses the given Schema descriptor, assigns indices and finds the descriptor for given Index.
     *
     * @param schemaDescriptor Protobuf Schema Descriptor
     * @param indexToFind Index for the descriptor.
     * @return descriptor Protobuf descriptor matching the index.
     */
    public Descriptors.Descriptor getByIndex(
        @NonNull Descriptors.FileDescriptor schemaDescriptor,
        @NonNull Integer indexToFind) {

        final BiMap<Integer, Descriptors.Descriptor> indexToDescriptorBiMap = getAll(schemaDescriptor).inverse();

        if (!indexToDescriptorBiMap.containsKey(indexToFind)) {
            final String errorMessage = String.format(
                "No corresponding descriptor found for the index: %d", indexToFind);
            throw new AWSSchemaRegistryException(errorMessage);
        }

        return indexToDescriptorBiMap.get(indexToFind);
    }

    /**
     * Parse the Protobuf Schema descriptor using level-order traversal,
     * sort the descriptors lexicographically and assign an index for each message type.
     * <p>
     * TODO: Referencing other proto schemas using import statements is not supported yet.
     * https://github.com/awslabs/aws-glue-schema-registry/issues/32
     * <p>
     * Example:
     * <code>
     * message B {
     *  message C {
     *
     *  }
     *  message A {
     *   message D {
     *
     *   }
     *  }
     * }
     * </code>
     * <p>
     * Assigned indices:
     * B = 0, B.A = 1, B.A.D = 2, B.C = 3
     *
     * @param schemaDescriptor Protobuf Schema Descriptor.
     * @return Set of MessageIndex
     */
    @VisibleForTesting
    protected BiMap<Descriptors.Descriptor, Integer> getAll(@NonNull Descriptors.FileDescriptor schemaDescriptor) {
        final Queue<Descriptors.Descriptor> descriptorQueue = new LinkedList<>();
        final List<Descriptors.Descriptor> allDescriptors = new ArrayList<>();

        final List<Descriptors.Descriptor> parentLevelTypes = schemaDescriptor.getMessageTypes();
        descriptorQueue.addAll(parentLevelTypes);

        while (!descriptorQueue.isEmpty()) {
            final Descriptors.Descriptor descriptor = descriptorQueue.remove();
            allDescriptors.add(descriptor);

            final List<Descriptors.Descriptor> nestedDescriptors = descriptor.getNestedTypes();
            descriptorQueue.addAll(nestedDescriptors);
        }

        //Sort descriptor names by lexicographical order and assign an index.
        allDescriptors.sort(comparing(Descriptors.Descriptor::getFullName));

        final BiMap<Descriptors.Descriptor, Integer> messageIndices = HashBiMap.create(allDescriptors.size());
        for (int index = 0; index < allDescriptors.size(); index++) {
            messageIndices.put(allDescriptors.get(index), index);
        }

        return messageIndices;
    }
}
