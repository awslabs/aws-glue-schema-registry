package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf;

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ConnectSchemaToProtobufSchemaConverter;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.CommonTestHelper.createConnectSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;

public class ToProtobufTestDataGenerator {
    private static final String TEST_RESOURCE_PATH = "src/test/resources/";

    public static Schema getPrimitiveSchema(String name) {
        return createConnectSchema(name, getPrimitiveTypes(), ImmutableMap.of());
    }

    @SneakyThrows
    public static String getProtobufSchema(String fileName) {
        return new String(Files.readAllBytes(Paths.get(TEST_RESOURCE_PATH, fileName)),
            Charset.defaultCharset());
    }

    @SneakyThrows
    public static DynamicMessage getProtobufPrimitiveMessage() {
        Descriptors.FileDescriptor fileDescriptor = getPrimitiveFileDescriptor();
        Descriptors.Descriptor descriptor = fileDescriptor.getMessageTypes().get(0);
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);
        Function<String, Descriptors.FieldDescriptor> field = descriptor::findFieldByName;

        return dynamicMessageBuilder
            .setField(descriptor.findFieldByName("i8"), 2)
            .setField(field.apply("i8WithParam"), 0)
            //This shouldn't be set by the converter as it's null
//            .setField(field.apply("i8Optional"), null)
            .setField(field.apply("i8WithDefault"), 10)
            .setField(field.apply("i16"), 255)
            .setField(field.apply("i16WithParam"), 234)
            .setField(field.apply("i16WithDefault"), 15)
            .setField(field.apply("i16Optional"), 87)
            .setField(field.apply("i32"), 123123)
            .setField(field.apply("i32WithParam"), 23982)
            .setField(field.apply("i32WithSameTypeMetadata"), 2345)
            .setField(field.apply("i32WithMetadata"), -1829)
            .setField(field.apply("i32WithAnotherMetadata"), 123)
            .setField(field.apply("i32WithDefault"), 0)
            //This shouldn't be set by the converter as it's null
//            .setField(field.apply("i32Optional"), null)
            .setField(field.apply("i64"), -23499L)
            .setField(field.apply("i64WithParam"), 7659L)
            .setField(field.apply("i64WithDefault"), 1238102931L)
            .setField(field.apply("i64WithSameTypeMetadata"), 8294L)
            .setField(field.apply("i64WithMetadata"), 9123L)
            .setField(field.apply("i64WithAnotherMetadata"), 8272L)
            .setField(field.apply("i64WithYetAnotherMetadata"), 80123)
            .setField(field.apply("i64Optional"), 91010L)
            .setField(field.apply("f32"), 34.56F)
            .setField(field.apply("f32WithParam"), 89.00F)
            .setField(field.apply("f32Optional"), 81232.1234566F)
            .setField(field.apply("f32WithDefault"), 2456F)
            .setField(field.apply("f64"), 9123D)
            .setField(field.apply("f64WithParam"), 91202.213D)
            .setField(field.apply("f64Optional"), -927.456D)
            .setField(field.apply("f64WithDefault"), 0.0023D)
            .setField(field.apply("bool"), true)
            .setField(field.apply("boolWithParam"), true)
            //This shouldn't be set by the converter as it's null
//            .setField(field.apply("boolOptional"), null)
            .setField(field.apply("boolWithDefault"), false)
            .setField(field.apply("bytes"), ByteString.copyFrom(new byte [] {1,5,6,7}))
            .setField(field.apply("bytesWithParam"), ByteString.copyFrom(new byte[] {1}))
            //This shouldn't be set by the converter as it's null
//            .setField(field.apply("bytesOptional"), null)
            .setField(field.apply("bytesWithDefault"), ByteString.copyFrom(ByteBuffer.wrap(new byte[] {1,4,5,6})))
            .setField(field.apply("str"), "asdsai131")
            .setField(field.apply("strWithParam"), "12351")
            //This shouldn't be set by the converter as it's null
//            .setField(field.apply("strOptional"), null)
            .setField(field.apply("strWithDefault"), "")
            .build();
    }

    private static Descriptors.FileDescriptor getPrimitiveFileDescriptor() {
        return new ConnectSchemaToProtobufSchemaConverter().convert(getPrimitiveSchema("primitiveProtobufSchema"));
    }

    public static Struct getPrimitiveTypesData() {
        Schema connectSchema = createConnectSchema("primitiveProtobufSchema", getPrimitiveTypes(), ImmutableMap.of());
        final Struct connectData = new Struct(connectSchema);

        connectData
            .put("i8", (byte) 2)
            .put("i8WithParam", (byte) 0)
            .put("i8Optional", null)
            .put("i8WithDefault", (byte) 10)
            .put("i16", (short) 255)
            .put("i16WithParam", (short) 234)
            .put("i16WithDefault", (short) 15)
            .put("i16Optional", (short) 87)
            .put("i32", 123123)
            .put("i32WithParam", 23982)
            .put("i32WithSameTypeMetadata", 2345)
            .put("i32WithMetadata", -1829)
            .put("i32WithAnotherMetadata", 123)
            .put("i32WithDefault", 0)
            .put("i32Optional", null)
            .put("i64", -23499L)
            .put("i64WithParam", 7659L)
            .put("i64WithDefault", 1238102931L)
            .put("i64WithSameTypeMetadata", 8294L)
            .put("i64WithMetadata", 9123L)
            .put("i64WithAnotherMetadata", 8272L)
            .put("i64WithYetAnotherMetadata", 80123L)
            .put("i64Optional", 91010L)
            .put("f32", 34.56F)
            .put("f32WithParam", 89.00F)
            .put("f32Optional", 81232.1234566F)
            .put("f32WithDefault", 2456F)
            .put("f64", 9123D)
            .put("f64WithParam", 91202.213D)
            .put("f64Optional", -927.456D)
            .put("f64WithDefault", 0.0023D)
            .put("bool", true)
            .put("boolWithParam", true)
            .put("boolOptional", null)
            .put("boolWithDefault", false)
            .put("bytes", new byte [] {1,5,6,7})
            .put("bytesWithParam", new byte[] {1})
            .put("bytesOptional", null)
            .put("bytesWithDefault", ByteBuffer.wrap(new byte[] {1,4,5,6}))
            .put("str", "asdsai131")
            .put("strWithParam", "12351")
            .put("strOptional", null)
            .put("strWithDefault", "");
        return connectData;
    }

    private static Map<String, Schema> getPrimitiveTypes() {
        return ImmutableMap.<String, Schema>builder()
            .put("i8", new SchemaBuilder(Schema.Type.INT8).build())
            .put("i8WithParam", new SchemaBuilder(Schema.Type.INT8).parameter(PROTOBUF_TAG, "2000").build())
            .put("i8Optional", new SchemaBuilder(Schema.Type.INT8).optional().build())
            .put("i8WithDefault", new SchemaBuilder(Schema.Type.INT8).defaultValue((byte) 1).build())
            .put("i16", new SchemaBuilder(Schema.Type.INT16).build())
            .put("i16WithParam", new SchemaBuilder(Schema.Type.INT16).parameter(PROTOBUF_TAG, "4123").build())
            .put("i16WithDefault", new SchemaBuilder(Schema.Type.INT16).defaultValue((short) 890).build())
            .put("i16Optional", new SchemaBuilder(Schema.Type.INT16).optional().build())
            .put("i32", new SchemaBuilder(Schema.Type.INT32).build())
            .put("i32WithParam", new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TAG, "8123").build())
            .put("i32WithSameTypeMetadata",
                new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "INT32").build())
            .put("i32WithMetadata", new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "sint32").build())
            .put("i32WithAnotherMetadata",
                new SchemaBuilder(Schema.Type.INT32).parameter(PROTOBUF_TYPE, "sfixed32").build())
            .put("i32WithDefault", new SchemaBuilder(Schema.Type.INT32).defaultValue(21233).build())
            .put("i32Optional", new SchemaBuilder(Schema.Type.INT32).optional().build())
            .put("i64", new SchemaBuilder(Schema.Type.INT64).build())
            .put("i64WithParam", new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TAG, "9123").build())
            .put("i64WithDefault", new SchemaBuilder(Schema.Type.INT64).defaultValue(Long.MAX_VALUE).build())
            .put("i64WithSameTypeMetadata",
                new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TYPE, "int64").build())
            .put("i64WithMetadata", new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TYPE, "sint64").build())
            .put("i64WithAnotherMetadata",
                new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TYPE, "fixed64").build())
            .put("i64WithYetAnotherMetadata",
                new SchemaBuilder(Schema.Type.INT64).parameter(PROTOBUF_TYPE, "uint32").build())
            .put("i64Optional", new SchemaBuilder(Schema.Type.INT64).optional().build())
            .put("f32", new SchemaBuilder(Schema.Type.FLOAT32).build())
            .put("f32WithParam", new SchemaBuilder(Schema.Type.FLOAT32).parameter(PROTOBUF_TAG, "10923").build())
            .put("f32Optional", new SchemaBuilder(Schema.Type.FLOAT32).optional().build())
            .put("f32WithDefault", new SchemaBuilder(Schema.Type.FLOAT32).defaultValue(123.2324f).build())
            .put("f64", new SchemaBuilder(Schema.Type.FLOAT64).build())
            .put("f64WithParam", new SchemaBuilder(Schema.Type.FLOAT64).parameter(PROTOBUF_TAG, "1112").build())
            .put("f64Optional", new SchemaBuilder(Schema.Type.FLOAT64).optional().build())
            .put("f64WithDefault", new SchemaBuilder(Schema.Type.FLOAT64).defaultValue(123112322232.2644).build())
            .put("bool", new SchemaBuilder(Schema.Type.BOOLEAN).build())
            .put("boolWithParam", new SchemaBuilder(Schema.Type.BOOLEAN).parameter(PROTOBUF_TAG, "12123").build())
            .put("boolOptional", new SchemaBuilder(Schema.Type.BOOLEAN).optional().build())
            .put("boolWithDefault", new SchemaBuilder(Schema.Type.BOOLEAN).defaultValue(true).build())
            .put("bytes", new SchemaBuilder(Schema.Type.BYTES).build())
            .put("bytesWithParam", new SchemaBuilder(Schema.Type.BYTES).parameter(PROTOBUF_TAG, "18924").build())
            .put("bytesOptional", new SchemaBuilder(Schema.Type.BYTES).optional().build())
            .put("bytesWithDefault",
                new SchemaBuilder(Schema.Type.BYTES).defaultValue(new byte[] { 3, 4, 'f', 123 }).build())
            .put("str", new SchemaBuilder(Schema.Type.STRING).build())
            .put("strWithParam", new SchemaBuilder(Schema.Type.STRING).parameter(PROTOBUF_TAG, "13912").build())
            .put("strOptional", new SchemaBuilder(Schema.Type.STRING).optional().build())
            .put("strWithDefault", new SchemaBuilder(Schema.Type.STRING).defaultValue("foobarxyz").build())
            .build();
    }

    public static Schema getArraySchema(String name) {
        return createConnectSchema(name, getArrayType(), ImmutableMap.of());
    }

    @SneakyThrows
    public static DynamicMessage getProtobufArrayMessage() {
        Descriptors.FileDescriptor fileDescriptor = getArrayFileDescriptor();
        Descriptors.Descriptor descriptor = fileDescriptor.getMessageTypes().get(0);
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);
        Function<String, Descriptors.FieldDescriptor> field = descriptor::findFieldByName;

        return dynamicMessageBuilder
            .setField(field.apply("str"), Arrays.asList("foo", "bar", "baz"))
            .setField(field.apply("boolean"), Arrays.asList(true, false))
            .setField(field.apply("i32"), new ArrayList<>())
            .build();
    }

    private static Descriptors.FileDescriptor getArrayFileDescriptor() {
        return new ConnectSchemaToProtobufSchemaConverter().convert(getArraySchema("arrayProtobufSchema"));
    }

    public static Struct getArrayTypeData() {
        Schema connectSchema = createConnectSchema("arrayProtobufSchema", getArrayType(), ImmutableMap.of());
        final Struct connectData = new Struct(connectSchema);

        connectData
            .put("str", Arrays.asList("foo", "bar", "baz"))
            .put("boolean", Arrays.asList(true, false))
            .put("i32", new ArrayList<>());
        return connectData;
    }

    private static Map<String, Schema> getArrayType() {
        return ImmutableMap.<String, Schema>builder()
            .put("str", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .put("i32", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .put("boolean", SchemaBuilder.array(Schema.BOOLEAN_SCHEMA).build())
            .build();
    }

    public static Schema getMapSchema(String name) {
        return createConnectSchema(name, getMapType(), ImmutableMap.of());
    }

    @SneakyThrows
    public static DynamicMessage getProtobufMapMessage() {
        Descriptors.FileDescriptor fileDescriptor = getMapFileDescriptor();
        Descriptors.Descriptor descriptor = fileDescriptor.getMessageTypes().get(0);
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);

        Descriptors.Descriptor intMapDescriptor = descriptor.findNestedTypeByName("IntMapEntry");
        DynamicMessage.Builder intMapBuilder = DynamicMessage.newBuilder(intMapDescriptor)
            .setField(intMapDescriptor.findFieldByName("key"), 2)
            .setField(intMapDescriptor.findFieldByName("value"), 22);

        Descriptors.Descriptor boolMapDescriptor = descriptor.findNestedTypeByName("BoolMapEntry");
        DynamicMessage.Builder boolMapBuilder = DynamicMessage.newBuilder(boolMapDescriptor)
            .setField(boolMapDescriptor.findFieldByName("key"), "A")
            .setField(boolMapDescriptor.findFieldByName("value"), true);
        DynamicMessage.Builder boolMapBuilder2 = DynamicMessage.newBuilder(boolMapDescriptor)
            .setField(boolMapDescriptor.findFieldByName("key"), "B")
            .setField(boolMapDescriptor.findFieldByName("value"), false);

        return dynamicMessageBuilder
            .addRepeatedField(descriptor.findFieldByName("intMap"), intMapBuilder.build())
            .addRepeatedField(descriptor.findFieldByName("boolMap"), boolMapBuilder.build())
            .addRepeatedField(descriptor.findFieldByName("boolMap"), boolMapBuilder2.build())
            .build();
    }

    private static Descriptors.FileDescriptor getMapFileDescriptor() {
        return new ConnectSchemaToProtobufSchemaConverter().convert(getMapSchema("mapProtobufSchema"));
    }

    public static Struct getMapTypeData() {
        Schema connectSchema = createConnectSchema("mapProtobufSchema", getMapType(), ImmutableMap.of());
        final Struct connectData = new Struct(connectSchema);

        connectData
            .put("intMap", Collections.singletonMap(2, 22))
            .put("boolMap", ImmutableMap.of("A", true, "B", false))
            .put("strMap", new HashMap<>());
        return connectData;
    }

    private static Map<String, Schema> getMapType() {
        return ImmutableMap.<String, Schema>builder()
            .put("intMap", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build())
            .put("boolMap", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).build())
            .put("strMap", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
            .build();
    }
}
