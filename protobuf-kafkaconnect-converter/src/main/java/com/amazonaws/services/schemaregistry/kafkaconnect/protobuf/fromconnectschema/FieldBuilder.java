/*
 * Copyright 2022 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema;

import com.google.protobuf.DescriptorProtos;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_NAME;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ENUM_VALUE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_ONEOF_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getSchemaSimpleName;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.getTypeName;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterUtils.isEnumType;

/**
 * Builds the fields into given message and fileDescriptorProto.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FieldBuilder {
    public static void build(
        final Schema schema,
        final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder,
        final DescriptorProtos.DescriptorProto.Builder messageDescriptorProtoBuilder) {

        //Sequentially add tag numbers to fields as they appear in original schema starting with 1.
        final AtomicInteger tagNumber = new AtomicInteger(1);
        List<String> syntheticOneofs = new ArrayList<>();
        Map<String, DescriptorProtos.FieldDescriptorProto.Builder> fieldBuilderMap = new LinkedHashMap<>();
        for (final Field field : schema.fields()) {
            final Schema fieldSchema = field.schema();
            final String fieldName = field.name();

            //Get the corresponding type converter and convert it.
            if (isEnumType(fieldSchema)) {
                final Map<String, String> schemaParams = fieldSchema.parameters();
                final String enumFullName = schemaParams.get(PROTOBUF_ENUM_NAME);
                final String enumName = getSchemaSimpleName(enumFullName);

                final DescriptorProtos.EnumDescriptorProto.Builder enumDescriptorProtoBuilder =
                        DescriptorProtos.EnumDescriptorProto.newBuilder().setName(enumName);
                for (Map.Entry<String, String> parameter : schemaParams.entrySet()) {
                    String parameterKey = parameter.getKey();
                    if (parameterKey.startsWith(PROTOBUF_ENUM_VALUE)) {
                        enumDescriptorProtoBuilder.addValue(
                                DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                                        .setName(parameterKey.replace(PROTOBUF_ENUM_VALUE, ""))
                                        .setNumber(Integer.parseInt(parameter.getValue()))
                                        .build()
                        );
                    }
                }

                //Adding the Enum to the protobuf schema file, and defining a field as Enum
                if (isParentLevel(fileDescriptorProtoBuilder.getPackage(), enumFullName)) {
                    fileDescriptorProtoBuilder.addEnumType(enumDescriptorProtoBuilder);
                } else {
                    messageDescriptorProtoBuilder.addEnumType(enumDescriptorProtoBuilder);
                }
            } else if (Schema.Type.MAP.equals(fieldSchema.type())) {
                String mapEntryName = ProtobufSchemaConverterUtils.toMapEntryName(fieldName);
                messageDescriptorProtoBuilder.addNestedType(buildMap(fieldSchema, mapEntryName,
                    fileDescriptorProtoBuilder, messageDescriptorProtoBuilder));
            } else if (Schema.Type.STRUCT.equals(fieldSchema.type())) {
                if (fieldSchema.parameters().containsKey(PROTOBUF_TYPE)
                        && fieldSchema.parameters().get(PROTOBUF_TYPE).equals(PROTOBUF_ONEOF_TYPE)) {
                    buildOneof(fieldSchema, fieldName, tagNumber, fileDescriptorProtoBuilder,
                            messageDescriptorProtoBuilder, fieldBuilderMap);
                    continue;
                }

                // Convert the Struct type schema to a Protobuf message schema
                DescriptorProtos.DescriptorProto.Builder nestedMessageDescriptorProtoBuilder =
                        DescriptorProtos.DescriptorProto.newBuilder();
                nestedMessageDescriptorProtoBuilder.setName(getSchemaSimpleName(fieldSchema.name()));
                build(fieldSchema, fileDescriptorProtoBuilder, nestedMessageDescriptorProtoBuilder);
                // If schema is at parent level, Protobuf message is added as a message type
                // If schema is not at parent level, Protobuf message is added as a nested type
                if (isParentLevel(fileDescriptorProtoBuilder.getPackage(), fieldSchema.name())) {
                    fileDescriptorProtoBuilder.addMessageType(nestedMessageDescriptorProtoBuilder);
                } else {
                    messageDescriptorProtoBuilder.addNestedType(nestedMessageDescriptorProtoBuilder);
                }
            }

            DescriptorProtos.FieldDescriptorProto.Builder fieldDescriptorProtoBuilder =
                getFieldDescriptorProtoBuilder(fieldSchema, fieldName, fileDescriptorProtoBuilder,
                    messageDescriptorProtoBuilder);
            fieldDescriptorProtoBuilder.setNumber(
                tagNumberFromMetadata(fieldSchema.parameters()).orElseGet(tagNumber::getAndIncrement)
            );
            //Proto3 Optional helps distinguish between non-existing and empty values.
            if (fieldSchema.isOptional() && !fieldSchema.type().equals(Schema.Type.ARRAY)) {
                syntheticOneofs.add(fieldName);
            }
            fieldBuilderMap.put(fieldName, fieldDescriptorProtoBuilder);
        }

        // Synthetic oneofs must be ordered after all "real" oneofs
        for (String syntheticOneofName : syntheticOneofs) {
            setProto3Optional(fieldBuilderMap.get(syntheticOneofName), messageDescriptorProtoBuilder);
        }
        for (Map.Entry<String, DescriptorProtos.FieldDescriptorProto.Builder> entry : fieldBuilderMap.entrySet()) {
            messageDescriptorProtoBuilder.addField(entry.getValue());
        }
    }

    /**
     * Protobuf map is built from two parts: the map field and the nested type for the map entry
     * The nested type for the map entry is constructed as follows:
     * 1. Key optional field is added to the nested type with field number 1
     * 2. Value optional field is added to the nested type with field number 2
     * 3. MapEntry option is set as true in the nested type
     */
    private static DescriptorProtos.DescriptorProto buildMap(Schema schema, String name,
         final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder,
         final DescriptorProtos.DescriptorProto.Builder messageDescriptorProtoBuilder) {
        DescriptorProtos.FieldDescriptorProto.Builder keyFieldBuilder =
            getFieldDescriptorProtoBuilder(schema.keySchema(), "key", fileDescriptorProtoBuilder,
                messageDescriptorProtoBuilder);
        keyFieldBuilder.setNumber(1);
        DescriptorProtos.FieldDescriptorProto.Builder valueFieldBuilder =
            getFieldDescriptorProtoBuilder(schema.valueSchema(), "value", fileDescriptorProtoBuilder,
                messageDescriptorProtoBuilder);
        valueFieldBuilder.setNumber(2);

        DescriptorProtos.DescriptorProto.Builder mapBuilder =
            DescriptorProtos.DescriptorProto.newBuilder().setName(name);
        mapBuilder.addField(keyFieldBuilder.build());
        mapBuilder.addField(valueFieldBuilder.build());

        DescriptorProtos.MessageOptions.Builder optionsBuilder = DescriptorProtos.MessageOptions.newBuilder()
                .setMapEntry(true);
        mapBuilder.mergeOptions(optionsBuilder.build());

        return mapBuilder.build();
    }

    /**
     * Protobuf Oneof is constructed as follows:
     * 1. Oneof declaration is added in the message
     * 2. For each oneof field, it is added as an optional field in the message with oneof index associated to the same
     * oneof declaration
     */
    private static void buildOneof(Schema schema, String name, AtomicInteger tagNumber,
                                   final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder,
                                   final DescriptorProtos.DescriptorProto.Builder messageDescriptorProtoBuilder,
                                   Map<String, DescriptorProtos.FieldDescriptorProto.Builder> fieldBuilderMap) {
        messageDescriptorProtoBuilder.addOneofDecl(
                DescriptorProtos.OneofDescriptorProto
                        .newBuilder()
                        .setName(name)
                        .build());
        for (final Field oneofField: schema.fields()) {
            DescriptorProtos.FieldDescriptorProto.Builder oneofFieldDescriptorProtoBuilder =
                    getFieldDescriptorProtoBuilder(oneofField.schema(), oneofField.name(),
                            fileDescriptorProtoBuilder, messageDescriptorProtoBuilder);
            oneofFieldDescriptorProtoBuilder.setNumber(
                    tagNumberFromMetadata(oneofField.schema().parameters()).orElseGet(tagNumber::getAndIncrement)
            );
            oneofFieldDescriptorProtoBuilder.setOneofIndex(messageDescriptorProtoBuilder.getOneofDeclCount() - 1);
            fieldBuilderMap.put(oneofField.name(), oneofFieldDescriptorProtoBuilder);
        }
    }

    private static DescriptorProtos.FieldDescriptorProto.Builder getFieldDescriptorProtoBuilder(
            final Schema fieldSchema, final String fieldName,
            final DescriptorProtos.FileDescriptorProto.Builder fileDescriptorProtoBuilder,
            final DescriptorProtos.DescriptorProto.Builder messageDescriptorProtoBuilder) {

        final SchemaTypeConverter schemaTypeConverter = ConnectToProtobufTypeConverterFactory.get(fieldSchema);
        final DescriptorProtos.FieldDescriptorProto.Builder fieldDescriptorProtoBuilder =
                schemaTypeConverter
                        .toProtobufSchema(fieldSchema, messageDescriptorProtoBuilder, fileDescriptorProtoBuilder);

        if (Schema.Type.MAP.equals(fieldSchema.type())) {
            String typeName = getTypeName(fileDescriptorProtoBuilder.getPackage() + "."
                    + messageDescriptorProtoBuilder.getName() + "."
                    + ProtobufSchemaConverterUtils.toMapEntryName(fieldName));
            fieldDescriptorProtoBuilder.setTypeName(typeName);
        } else if (Schema.Type.STRUCT.equals(fieldSchema.type())) {
            String typeName = getTypeName(fieldSchema.name());
            fieldDescriptorProtoBuilder.setTypeName(typeName);
        }

        fieldDescriptorProtoBuilder.setName(fieldName);
        return fieldDescriptorProtoBuilder;
    }

    /**
     * Kafka Connect converters can have metadata defined to pre-assign tag numbers to certain fields.
     * This can be set using "awsgsr.protobuf.tag" property. We use it to get the tag number if present.
     */
    private static Optional<Integer> tagNumberFromMetadata(Map<String, String> schemaParams) {
        if (schemaParams == null || !schemaParams.containsKey(PROTOBUF_TAG)) {
            return Optional.empty();
        }

        final String tag = schemaParams.get(PROTOBUF_TAG);
        try {
            return Optional.of(Integer.parseInt(tag));
        } catch (Exception e) {
            throw new DataException("Cannot parse invalid Protobuf tag number metadata: " + tag);
        }
    }

    /**
     * Proto 3.15+ added support for Optionals. We will take advantage of it for converting
     * Connect optional schema fields.
     * A Proto3 optional adds a synthetic one-of definition to message
     * An example, for fieldName "foo", the optional one-of declaration will be,
     * oneof_decl {
     *     name: "_foo"
     * }
     */
    private static void setProto3Optional(
        final DescriptorProtos.FieldDescriptorProto.Builder fieldBuilder,
        final DescriptorProtos.DescriptorProto.Builder descriptorProtoBuilder) {

        descriptorProtoBuilder.addOneofDecl(
            DescriptorProtos.OneofDescriptorProto
                .newBuilder()
                .setName("_" + fieldBuilder.getName())
                .build());

        fieldBuilder.setProto3Optional(true);
        fieldBuilder.setOneofIndex(descriptorProtoBuilder.getOneofDeclCount() - 1);
    }

    /**
     * Schema name is in a complex form which consists of packageName, parent level schema simple name if exists,
     * and schema simple name itself
     * For example: message A { message B {} } message C {}
     * schema name for each will be
     * A -》"package.A", B -》"package.A.B", C -》"package.C"
     *
     * @param packageName package name of the protobuf schema
     * @param schemaName  schema name in the complex form
     * @return true if a schema is a parent level schema, false otherwise.
     */
    private static boolean isParentLevel(String packageName, String schemaName) {
        if (!schemaName.startsWith(packageName)) {
            return false;
        }
        String[] names = schemaName.split(packageName)[1].split("\\.");
        // If not nested schema, in other words parent level schema:
        // for example message A and message C, names should be ["", "A"] and ["", "C"]
        // If nested schema, in other words non parent level schema:
        // for example message B, names should be ["", "A", "B"]
        boolean isNotNested = names.length <= 2;
        return isNotNested;
    }
}
