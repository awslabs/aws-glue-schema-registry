package com.amazonaws.services.schemaregistry.deserializers.protobuf;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;

public class ProtobufClassName {
    /**
     * Derives the Protobuf generated Java class name using the Descriptor.
     * This logic is reverse-engineered from how the Protobuf compiler generates the Java classes.
     * Below method generates the class name from given sample schema,
     * ```
     * //File name: Sample.proto
     * package foo.bar.com;
     * message abc {
     *     message def {}
     * }
     * ```
     * Class name: `foo.bar.com.Sample$abc$def`
     *
     * Reference: https://github.com/protocolbuffers/protobuf/blob/4e0a1119c0c5cdfe89b54b9d66cb76223f17861a/src/google/protobuf/compiler/java/java_name_resolver.cc#L157
     * @param messageDescriptor Protobuf message descriptor
     * @return inferred class name.
     */
    public static String from(@NonNull final Descriptors.Descriptor messageDescriptor) {
        final Descriptors.FileDescriptor fileDescriptor = messageDescriptor.getFile();
        final DescriptorProtos.FileOptions fileOptions = fileDescriptor.getOptions();

        //Use 'java_package' if defined explicitly.
        final String packageString = fileOptions.hasJavaPackage() ?
            fileOptions.getJavaPackage() : fileDescriptor.getPackage();

        final String outerClassName = getOuterClassName(messageDescriptor);
        final String innerClassName = getInnerClassName(messageDescriptor);

        final boolean hasOuterClass = !outerClassName.isEmpty();
        final String classDelimiter = hasOuterClass ? "$" : "";

        final String relativeClassName = outerClassName + classDelimiter + innerClassName;

        final String packageDelimiter = ".";

        return packageString + packageDelimiter + relativeClassName;
    }

    private static String getOuterClassName(Descriptors.Descriptor descriptor) {
        final Descriptors.FileDescriptor fileDescriptor = descriptor.getFile();
        final DescriptorProtos.FileOptions fileOptions = fileDescriptor.getOptions();
        //If the Protobuf is compiled into multiple files,
        //the outer class name is not needed.
        if (fileOptions.getJavaMultipleFiles()) {
            return "";
        }

        if (fileOptions.hasJavaOuterClassname()) {
            return fileOptions.getJavaOuterClassname();
        }

        final String className = normalize(fileDescriptor.getFullName());

        //If className is same as descriptor name, `OuterClass` is suffixed by Protobuf compiler.
        if (className.equals(descriptor.getName())) {
            return className + "OuterClass";
        }

        return className;
    }

    private static String stripExtension(final String name) {
        int extensionIndex = name.lastIndexOf(".proto");
        if (extensionIndex == -1) {
            return name;
        }
        return name.substring(0, extensionIndex);
    }

    /**
     * Normalizes the fileDescriptorName to class name. Protobuf compiler replaces special characters and
     * camel cases it to generate the class name.
     * Reference: https://github.com/protocolbuffers/protobuf/blob/4e0a1119c0c5cdfe89b54b9d66cb76223f17861a/src/google/protobuf/compiler/java/java_helpers.cc#L159
     */
    public static String normalize(final String fileDescriptorName) {
        String strippedFileDescriptorName = stripExtension(fileDescriptorName);
        final String[] parts = strippedFileDescriptorName.split("[^a-zA-Z0-9]");
        final StringBuilder camelCaseName = new StringBuilder();
        for (String part : parts) {
            if (StringUtils.isBlank(part)) {
                continue;
            }
            if (part.length() == 1) {
                camelCaseName.append(part.toUpperCase());
                continue;
            }
            final String fixedCase = part.substring(0, 1).toUpperCase() + part.substring(1);
            camelCaseName.append(fixedCase);
        }
        if (strippedFileDescriptorName.lastIndexOf('#') == strippedFileDescriptorName.length() - 1) {
            camelCaseName.append("_");
        }
        return camelCaseName.toString();
    }

    /**
     * We are constructing the inner class name by traversing the container classes of message descriptor.
     * Ex: Consider a descriptor, 'F' which is part of nested class structure 'A.B.C.D.E.F'.
     * We need to construct the following to find the corresponding Java class, 'A$B$C$D$E$F'
     */
    private static String getInnerClassName(final Descriptors.Descriptor messageDescriptor) {
        final StringBuilder inner = new StringBuilder();
        inner.insert(0, messageDescriptor.getName());

        Descriptors.Descriptor descriptor = messageDescriptor.getContainingType();
        while (descriptor != null) {
            inner.insert(0, descriptor.getName() + "$");
            descriptor = descriptor.getContainingType();
        }

        return inner.toString();
    }
}
