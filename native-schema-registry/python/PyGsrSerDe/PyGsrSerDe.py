from .GsrSerDe import *

"""
TODO: Add documentation and tests.
TODO: Add validations
"""


class GlueSchemaRegistrySchema:
    def __init__(self, schema_name, schema_def, data_format):
        self.gsr_schema = glue_schema_registry_schema(schema_name, schema_def, data_format)

    def __del__(self):
        del self.gsr_schema

    def schema_name(self):
        return self.gsr_schema.get_schema_name()

    def data_format(self):
        return self.gsr_schema.get_data_format()

    def schema_def(self):
        return self.gsr_schema.get_schema_def()


class GlueSchemaRegistrySerializer:
    def __init__(self):
        self.serializer = glue_schema_registry_serializer()

    def __del__(self):
        del self.serializer

    def encode(self, transport_name: str, schema: GlueSchemaRegistrySchema, byte_arr: bytes) -> bytes:
        ro_byte_array = read_only_byte_array(byte_arr)
        encoded_byte_buffer = self.serializer.encode(ro_byte_array, transport_name, schema.gsr_schema)
        return encoded_byte_buffer
