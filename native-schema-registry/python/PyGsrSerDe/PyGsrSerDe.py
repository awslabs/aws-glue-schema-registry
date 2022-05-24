from PyGsrSerDe import _GsrSerDe

from .GsrSerDe import *

"""
TODO: Add documentation and tests.
TODO: Add validations
"""


class GlueSchemaRegistrySchema:
    def __init__(self, schema_name, schema_def, data_format):
        self.schema_name = schema_name
        self.schema_def = schema_def
        self.data_format = data_format

    def __str__(self):
        return f'GlueSchemaRegistrySchema: [{self.schema_name}, {self.schema_def}, {self.data_format}]'


class GlueSchemaRegistrySerializer:
    def __init__(self):
        self.serializer = glue_schema_registry_serializer()

    def __del__(self):
        del self.serializer

    def encode(self, transport_name: str, schema: GlueSchemaRegistrySchema, byte_arr: bytes) -> bytes:
        ro_byte_array = read_only_byte_array(byte_arr)
        gsr_schema = glue_schema_registry_schema(schema.schema_name, schema.schema_def, schema.data_format)
        encoded_byte_buffer = self.serializer.encode(ro_byte_array, transport_name, gsr_schema)
        return encoded_byte_buffer


class GlueSchemaRegistryDeserializer:
    def __init__(self):
        self.deserializer = glue_schema_registry_deserializer()

    def __del__(self):
        del self.deserializer

    def decode(self, byte_arr: bytes) -> bytes:
        ro_byte_array = read_only_byte_array(byte_arr)
        decoded_byte_buffer = self.deserializer.decode(ro_byte_array)
        return decoded_byte_buffer

    def decode_schema(self, byte_arr: bytes) -> GlueSchemaRegistrySchema:
        ro_byte_array = read_only_byte_array(byte_arr)
        gsr_schema = self.deserializer.decode_schema(ro_byte_array)
        schema = GlueSchemaRegistrySchema(
             gsr_schema.get_schema_name(),
             gsr_schema.get_schema_def(),
             gsr_schema.get_data_format(),
        )
        return schema

    def can_decode(self, byte_arr: bytes) -> bool:
        ro_byte_array = read_only_byte_array(byte_arr)
        return self.deserializer.can_decode(ro_byte_array)
