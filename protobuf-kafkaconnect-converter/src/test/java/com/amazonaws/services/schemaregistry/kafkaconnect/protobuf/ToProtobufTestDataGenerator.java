package com.amazonaws.services.schemaregistry.kafkaconnect.protobuf;

import com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ConnectSchemaToProtobufSchemaConverter;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.SneakyThrows;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Decimal;

import java.math.BigDecimal;
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

import additionalTypes.Decimals;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.CommonTestHelper.createConnectSchema;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TAG;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.PROTOBUF_TYPE;
import static com.amazonaws.services.schemaregistry.kafkaconnect.protobuf.fromconnectschema.ProtobufSchemaConverterConstants.DECIMAL_DEFAULT_SCALE;

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

    public static Schema getEnumSchema(String name) {
        return createConnectSchema(name, getEnumType(), ImmutableMap.of());
    }

    @SneakyThrows
    public static DynamicMessage getProtobufEnumMessage() {
        Descriptors.FileDescriptor fileDescriptor = getEnumFileDescriptor();
        Descriptors.Descriptor descriptor = fileDescriptor.getMessageTypes().get(0);
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);

        return dynamicMessageBuilder
                .setField(descriptor.findFieldByName("corpus"), fileDescriptor.findEnumTypeByName("corpus").findValueByName("UNIVERSAL"))
                .setField(descriptor.findFieldByName("shapes"), fileDescriptor.findEnumTypeByName("shapes").findValueByName("TRIANGLE"))
                .setField(descriptor.findFieldByName("color"), fileDescriptor.findEnumTypeByName("color").findValueByName("BLUE"))
                .setField(descriptor.findFieldByName("fruits"), fileDescriptor.findEnumTypeByName("fruits").findValueByName("BANANA"))
                .build();
    }

    private static Descriptors.FileDescriptor getEnumFileDescriptor() {
        return new ConnectSchemaToProtobufSchemaConverter().convert(getEnumSchema("enumProtobufSchema"));
    }

    public static Struct getEnumTypeData() {
        Schema connectSchema = createConnectSchema("enumProtobufSchema", getEnumType(), ImmutableMap.of());
        final Struct connectData = new Struct(connectSchema);

        connectData
                .put("corpus", "UNIVERSAL")
                .put("shapes", "TRIANGLE")
                .put("color", "BLUE");
        //.put("fruits", "BANANA"); //Unset to check default value

        return connectData;
    }

    private static Map<String, Schema> getEnumType() {
        return ImmutableMap.<String, Schema>builder()
                .put("corpus", new SchemaBuilder(Schema.Type.STRING)
                        .parameter("protobuf.type", "enum")
                        .parameter("PROTOBUF_ENUM_VALUE.UNIVERSAL", "0")
                        .parameter("PROTOBUF_ENUM_VALUE.WEB", "1")
                        .parameter("PROTOBUF_ENUM_VALUE.NEWS", "4")
                        .parameter("PROTOBUF_ENUM_VALUE.IMAGES", "2")
                        .parameter("PROTOBUF_ENUM_VALUE.LOCAL", "3")
                        .parameter("PROTOBUF_ENUM_VALUE.PRODUCTS", "5")
                        .parameter("PROTOBUF_ENUM_VALUE.VIDEO", "6")
                        .parameter("ENUM_NAME", "corpus")
                        .parameter("protobuf.tag", "1")
                        .build())
                .put("shapes", new SchemaBuilder(Schema.Type.STRING)
                        .parameter("protobuf.type", "enum")
                        .parameter("PROTOBUF_ENUM_VALUE.SQUARE", "0")
                        .parameter("PROTOBUF_ENUM_VALUE.CIRCLE", "1")
                        .parameter("PROTOBUF_ENUM_VALUE.TRIANGLE", "2")
                        .parameter("ENUM_NAME", "shapes")
                        .parameter("protobuf.tag", "12345")
                        .build())
                .put("color", new SchemaBuilder(Schema.Type.STRING)
                        .parameter("protobuf.type", "enum")
                        .parameter("PROTOBUF_ENUM_VALUE.BLACK", "0")
                        .parameter("PROTOBUF_ENUM_VALUE.RED", "1")
                        .parameter("PROTOBUF_ENUM_VALUE.GREEN", "2")
                        .parameter("PROTOBUF_ENUM_VALUE.BLUE", "3")
                        .parameter("ENUM_NAME", "color")
                        .parameter("protobuf.tag", "2")
                        .optional()
                        .build())
                .put("fruits", new SchemaBuilder(Schema.Type.STRING).defaultValue("BANANA")
                        .parameter("protobuf.type", "enum")
                        .parameter("PROTOBUF_ENUM_VALUE.APPLE", "0")
                        .parameter("PROTOBUF_ENUM_VALUE.ORANGE", "1")
                        .parameter("PROTOBUF_ENUM_VALUE.BANANA", "2")
                        .parameter("ENUM_NAME", "fruits")
                        .parameter("protobuf.tag", "3")
                        .optional()
                        .build())
                .build();
    }



    public static Schema getTimeSchema(String name) {
        return createConnectSchema(name, getTimeTypes(), ImmutableMap.of());
    }

    @SneakyThrows
    public static DynamicMessage getProtobufTimeMessage() {
        Descriptors.FileDescriptor fileDescriptor = getTimeFileDescriptor();
        Descriptors.Descriptor descriptor = fileDescriptor.getMessageTypes().get(0);
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);

        com.google.type.Date.Builder dateBuilder = com.google.type.Date.newBuilder();
        dateBuilder.setYear(2022);
        dateBuilder.setMonth(3);
        dateBuilder.setDay(20);

        com.google.type.TimeOfDay.Builder todBuilder = com.google.type.TimeOfDay.newBuilder();
        todBuilder.setHours(2);
        todBuilder.setMinutes(2);
        todBuilder.setSeconds(42);

        com.google.protobuf.Timestamp.Builder timestampBuilder = com.google.protobuf.Timestamp.newBuilder();
        timestampBuilder.setSeconds(1);
        timestampBuilder.setNanos(805000000);

        return dynamicMessageBuilder
                .setField(descriptor.findFieldByName("date"), dateBuilder.build())
                .setField(descriptor.findFieldByName("time"), todBuilder.build())
                .setField(descriptor.findFieldByName("timestamp"), timestampBuilder.build())
                .build();
    }

    private static Descriptors.FileDescriptor getTimeFileDescriptor() {
        return new ConnectSchemaToProtobufSchemaConverter().convert(getTimeSchema("timeProtobufSchema"));
    }

    public static Struct getTimeTypeData() {
        Schema connectSchema = createConnectSchema("timeProtobufSchema", getTimeTypes(), ImmutableMap.of());
        final Struct connectData = new Struct(connectSchema);

        int dateDefVal = 19071; // equal to 2022/03/20 with reference to the unix epoch
        int timeDefVal = 7362000; // equal to 2 hours 2 minutes 42 seconds in millisecond
        long tsDefVal = 1805; // equal to 1 second 805000000 nanoseconds in millisecond
        java.util.Date date = Date.toLogical(Date.SCHEMA, dateDefVal);
        java.util.Date time = Time.toLogical(Time.SCHEMA, timeDefVal);
        java.util.Date timestamp = Timestamp.toLogical(Timestamp.SCHEMA, tsDefVal);

        connectData
                .put("date", date)
                .put("time", time)
                .put("timestamp", timestamp);
        return connectData;
    }

    private static Map<String, Schema> getTimeTypes() {
        return ImmutableMap.<String, Schema>builder()
                .put("date", Date.builder().doc("date field").build())
                .put("time", Time.builder().doc("time field").build())
                .put("timestamp", Timestamp.builder().doc("timestamp field").build())
                .build();
    }

    public static Schema getDecimalSchema(String name) {
        return createConnectSchema(name, getDecimalTypes(), ImmutableMap.of());
    }

    @SneakyThrows
    public static DynamicMessage getProtobufDecimalMessage() {
        Descriptors.FileDescriptor fileDescriptor = getDecimalFileDescriptor();
        Descriptors.Descriptor descriptor = fileDescriptor.getMessageTypes().get(0);
        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);

        Decimals.Decimal.Builder decimalBuilder = Decimals.Decimal.newBuilder();
        decimalBuilder.setUnits(1234);
        decimalBuilder.setFraction(567890000);
        decimalBuilder.setPrecision(9);
        decimalBuilder.setScale(5);

        return dynamicMessageBuilder
                .setField(descriptor.findFieldByName("decimal"), decimalBuilder.build())
                .build();
    }

    private static Descriptors.FileDescriptor getDecimalFileDescriptor() {
        return new ConnectSchemaToProtobufSchemaConverter().convert(getDecimalSchema("decimalProtobufSchema"));
    }

    public static Struct getDecimalTypeData() {
        Schema connectSchema = createConnectSchema("decimalProtobufSchema", getDecimalTypes(), ImmutableMap.of());
        final Struct connectData = new Struct(connectSchema);

        BigDecimal decimal = BigDecimal.valueOf(1234.56789);

        connectData
                .put("decimal", decimal);
        return connectData;
    }

    private static Map<String, Schema> getDecimalTypes() {
        return ImmutableMap.<String, Schema>builder()
                .put("decimal", Decimal.builder(DECIMAL_DEFAULT_SCALE))
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
