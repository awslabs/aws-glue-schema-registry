package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectdata;

import com.google.protobuf.Descriptors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DescriptorTree {
    /**
     * Do a level order traversal on the nested descriptors to construct a
     * map of absolute path of a descriptor to its descriptor.
     * Ex: message A { message B {} } message C {}
     * results in
     * {'.A' -》 A, '.A.B' -》 B, '.C' -》 C}
     */
    public static Map<String, Descriptors.Descriptor> parseAllDescriptors(
            final Descriptors.FileDescriptor fileDescriptor) {

        final String PARENT_PATH = ".";
        final Queue<DescriptorWithPath> traversalQueue = new LinkedList<>();
        final Map<String, Descriptors.Descriptor> messagesByName = new LinkedHashMap<>();

        //Add all the top level types to the queue to begin with.
        fileDescriptor
                .getMessageTypes()
                .stream()
                .map(descriptor -> new DescriptorWithPath(descriptor,
                        PARENT_PATH + descriptor.getName()))
                .forEach(traversalQueue::add);

        while (!traversalQueue.isEmpty()) {
            final DescriptorWithPath descriptorWithPath = traversalQueue.remove();

            final Descriptors.Descriptor descriptor = descriptorWithPath.getDescriptor();
            final String descriptorPath = descriptorWithPath.getPath();

            messagesByName.put(descriptorPath, descriptor);

            //Add the nested types to queue.
            descriptor
                    .getNestedTypes()
                    .stream()
                    .map(nestedDescriptor -> new DescriptorWithPath(nestedDescriptor,
                            descriptorPath + "." + nestedDescriptor.getName()))
                    .forEach(traversalQueue::add);
        }

        return messagesByName;
    }

    @Value
    private static class DescriptorWithPath {
        Descriptors.Descriptor descriptor;
        String path;
    }
}
