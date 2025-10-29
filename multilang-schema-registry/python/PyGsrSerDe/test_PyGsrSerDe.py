import unittest
from unittest import TestCase

from PyGsrSerDe import GlueSchemaRegistrySerializer, GlueSchemaRegistryDeserializer, GlueSchemaRegistrySchema, \
    AwsSchemaRegistryException

TRANSPORT_NAME = "SomeTransportName"
AVRO_SCHEMA = ""
DATA = bytes("asdas", "utf8")
SCHEMA_DEF = '{\"namespace\": \"example.avro\",\"type\": \"record\",\"name\": \"User\",\"fields\": [ {\"name\": ' \
             '\"name\", \"type\": \"string\"}, {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}, ' \
             '{\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]} ]} '
SCHEMA_NAME = "SomeSchemaName"

GSR_SCHEMA = GlueSchemaRegistrySchema(SCHEMA_NAME, SCHEMA_DEF, "AVRO")


class TestGlueSchemaRegistrySchema(TestCase):
    def setUp(self):
        self.serializer = GlueSchemaRegistrySerializer()
        self.deserializer = GlueSchemaRegistryDeserializer()

    def tearDown(self) -> None:
        del self.serializer
        del self.deserializer

    def test_serialization_deserialization_succeeds(self):
        encoded = self.serializer.encode(TRANSPORT_NAME, GSR_SCHEMA, DATA)
        self.assertTrue(len(encoded) != 0)
        self.assertTrue(self.deserializer.can_decode(encoded))

        decoded = self.deserializer.decode(encoded)
        self.assertTrue(len(decoded) != 0)

        decoded_schema = self.deserializer.decode_schema(encoded)
        self.assertEqual(GSR_SCHEMA, decoded_schema)

        self.assertEqual(DATA, decoded)

    def test_serializer_throws_exceptions(self):
        invalid_schema = GlueSchemaRegistrySchema("someName", "{}", "InvalidFormat")
        with self.assertRaisesRegex(AwsSchemaRegistryException, 'No enum constant software.amazon.awssdk.services.glue.model.DataFormat.InvalidFormat'):
            self.serializer.encode(TRANSPORT_NAME, invalid_schema, DATA)

    def test_serializer_throws_exceptions_for_invalid_inputs(self):
        invalid_bytes = b'12312'
        some_int = 23
        some_str = "wqe"
        for transport_name in [None, some_int, invalid_bytes]:
            with self.assertRaisesRegex(ValueError, 'transport_name should be one of types'):
                self.serializer.encode(transport_name, GSR_SCHEMA, DATA)

        for schema in [None, some_str, invalid_bytes]:
            with self.assertRaisesRegex(ValueError, 'schema should be one of types:'):
                self.serializer.encode(TRANSPORT_NAME, schema, DATA)

        for data in [b'', bytes(), bytearray(0)]:
            with self.assertRaisesRegex(ValueError, 'byte_array cannot be of length 0'):
                self.serializer.encode(TRANSPORT_NAME, GSR_SCHEMA, data)

        for data in [None, some_str, some_int]:
            with self.assertRaisesRegex(ValueError, 'byte_arr should be one of types: '):
                self.serializer.encode(TRANSPORT_NAME, GSR_SCHEMA, data)

    def test_deserializer_throws_exceptions(self):
        invalid_data = bytearray(10)
        expected_msg = 'Data is not compatible with schema registry size: 10'

        with self.assertRaisesRegex(AwsSchemaRegistryException, expected_msg):
            self.deserializer.decode(invalid_data)

        with self.assertRaisesRegex(AwsSchemaRegistryException, expected_msg):
            self.deserializer.decode_schema(invalid_data)
        self.assertFalse(self.deserializer.can_decode(invalid_data))

    def test_deserializer_throws_exceptions_for_Invalid_inputs(self):
        for data in [None, "SomeString", 0]:
            with self.assertRaisesRegex(ValueError, "byte_arr should be one of types: "):
                self.deserializer.decode(data)
                self.deserializer.decode_schema(data)
                self.deserializer.can_decode(data)

        for data in [bytes(), bytearray(0), b'']:
            with self.assertRaisesRegex(ValueError, 'byte_array cannot be of length 0'):
                self.deserializer.decode(data)
                self.deserializer.decode_schema(data)
                self.deserializer.can_decode(data)

    ##TODO: Temporary way to check for obvious leaks. We need to add build time checks.
    # def test_mem_test(self):
    #     while True:
    #         self.test_deserializer_throws_exceptions()
    #         self.test_serializer_throws_exceptions()
    #         self.test_serde()

    if __name__ == '__main__':
        unittest.main()
