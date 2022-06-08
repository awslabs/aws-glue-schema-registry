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

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getAllTypesSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getArraySchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getMapSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getOneofSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getStructSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getPrimitiveSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getEnumSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getTimeSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getDecimalSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.ToProtobufTestDataGenerator.getProtobufSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConnectSchemaToProtobufSchemaConverterTest {

    private static final ConnectSchemaToProtobufSchemaConverter CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER =
        new ConnectSchemaToProtobufSchemaConverter();

    private static Map<String, Schema> getPrimitiveTypesForExceptions() {
        return ImmutableMap.<String, Schema>builder()
            .put("nonNumberTag", new SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "jsf").build())
            .put("nullNumberTag", new SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, null).build())
            .put("invalidInt32Metadata", new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "int64").build())
            .put("invalidInt64Metadata",
                new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "string").build())
            .build();
    }

    private static Stream<Arguments> getConnectSchemaTestCases() {
        return Stream.of(
            Arguments.of(
                "PrimitiveTypes",
                getPrimitiveSchema("PrimitiveTypes"),
                getProtobufSchema("PrimitiveProtobufSchema.filedescproto")
            ),
            Arguments.of(
                "EnumType",
                getEnumSchema("EnumType"),
                getProtobufSchema("EnumProtobufSchema.filedescproto")
            ),
            Arguments.of(
                "ArrayType",
                getArraySchema("ArrayType"),
                getProtobufSchema("ArrayProtobufSchema.filedescproto")
            ),
            Arguments.of(
                "MapType",
                getMapSchema("MapType"),
                getProtobufSchema("MapProtobufSchema.filedescproto")
            ),
            Arguments.of(
                "TimeType",
                getTimeSchema("TimeType"),
                getProtobufSchema("TimeProtobufSchema.filedescproto")
            ),
            Arguments.of(
                "DecimalType",
                getDecimalSchema("DecimalType"),
                getProtobufSchema("DecimalProtobufSchema.filedescproto")
            ),
            Arguments.of(
                "NestedType",
                getStructSchema("NestedType"),
                getProtobufSchema("NestedProtobufSchema.filedescproto")
            ),
            Arguments.of(
                "OneofType",
                getOneofSchema("OneofType"),
                getProtobufSchema("OneofProtobufSchema.filedescproto")
            ),
            Arguments.of(
                "AllTypes",
                getAllTypesSchema("AllTypes"),
                getProtobufSchema("AllTypesProtobufSchema.filedescproto")
            )
        );
    }

    private static Stream<Arguments> getConnectSchemaExceptionTestCases() {
        return getPrimitiveTypesForExceptions()
            .entrySet()
            .stream()
            .map(entry ->
                Arguments.of(entry.getKey(), ImmutableMap.of(entry.getKey(), entry.getValue()))
            );
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("getConnectSchemaTestCases")
    public void fromConnectSchema_convertsConnectSchemaToProtobufSchema(String fileName, Schema connectSchema,
        String expectedProtobufSchema) {

        final Descriptors.FileDescriptor protobufSchema =
            CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema);

        final String actualSchema = protobufSchema.toProto().toString();

        assertEquals(expectedProtobufSchema, actualSchema);
    }

    @ParameterizedTest(name = "{index} {0}")
    @MethodSource("getConnectSchemaExceptionTestCases")
    public void fromConnectSchema_convertsConnectSchemaToProtobufSchema_forExceptions(String fileName,
        Map<String, Schema> connectTypes) {
        final SchemaBuilder parentSchemaBuilder = new SchemaBuilder(Schema.Type.STRUCT);
        parentSchemaBuilder.name(fileName);

        connectTypes
            .forEach(parentSchemaBuilder::field);

        final Schema connectSchema = parentSchemaBuilder.build();

        assertThrows(DataException.class,
            () -> CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(connectSchema));
    }

    @Test
    public void fromConnectSchema_onNullSchema_ThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> CONNECT_SCHEMA_TO_PROTOBUF_SCHEMA_CONVERTER.convert(null));
    }
}