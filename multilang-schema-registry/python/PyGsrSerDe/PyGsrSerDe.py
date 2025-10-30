from .GsrSerDe import *


class GlueSchemaRegistrySchema:
    """
    GlueSchemaRegistrySchema wraps the Glue Schema Registry Schema object.

    :param schema_name: Name of the Schema.
    :param schema_def: Definition of the Schema.
    :param data_format: Data format name, Ex: JSON, AVRO, Protobuf
    :return GlueSchemaRegistrySchema object.
    """
    def __init__(self, schema_name, schema_def, data_format):
        self.schema_name = schema_name
        self.schema_def = schema_def
        self.data_format = data_format

    def __str__(self):
        return f'GlueSchemaRegistrySchema: [{self.schema_name}, {self.schema_def}, {self.data_format}]'

    def __eq__(self, other):
        if other is None:
            return False
        return self.schema_name == other.schema_name and self.schema_def == other.schema_def and self.data_format == other.data_format


class AwsSchemaRegistryException(Exception):
    """
    Generic runtime exception to throw in case of serialization / de-serialization exceptions
    """
    pass


class GlueSchemaRegistrySerializer:
    """
    GlueSchemaRegistrySerializer instance is used to serialize (encode) given bytes with schema information.
    """
    def __init__(self):
        self.serializer = glue_schema_registry_serializer(None)

    def __del__(self):
        del self.serializer

    def encode(self, transport_name: str, schema: GlueSchemaRegistrySchema, byte_arr: bytes) -> bytes:
        """
        Encodes the given byte_arr with schema information.
        If configured, schema will be auto-registered with the schema registry service.
        Uses caching to prevent repeated calls to fetch the schemas.

        :param transport_name: (Optional) Transport name is used to add metadata during schema registration
        :param schema: GlueSchemaRegistrySchema object to fetch and encode into bytes.
        :param byte_arr: Byte array to encode.
        :return: Encoded byte array.
        :raises AwsSchemaRegistryException in case of errors.
        """
        validate_type('transport_name', transport_name, str)
        validate_type('schema', schema, GlueSchemaRegistrySchema)
        validate_byte_types('byte_arr', byte_arr)

        ro_byte_array = read_only_byte_array(byte_arr, None)
        gsr_schema = glue_schema_registry_schema(schema.schema_name, schema.schema_def, schema.data_format, None)
        try:
            mut_b_arr = self.serializer.encode(ro_byte_array, transport_name, gsr_schema, None)
            ret = bytearray(mut_b_arr.get_max_len())
            mut_b_arr.get_data_copy(ret)
            return ret
        except Exception as e:
            raise AwsSchemaRegistryException(e)


class GlueSchemaRegistryDeserializer:
    """
    GlueSchemaRegistryDeserializer instance is used to deserialize (decode) given bytes into original
    bytes and corresponding schemas.
    """
    def __init__(self):
        self.deserializer = glue_schema_registry_deserializer(None)

    def __del__(self):
        del self.deserializer

    def decode(self, byte_arr: bytes) -> bytes:
        """
        Decodes the encoded byte array into original byte array.
        :param byte_arr: Encoded byte array
        :return: Decoded byte array
        :raises AwsSchemaRegistryException in case of errors.
        """
        validate_byte_types('byte_arr', byte_arr)

        ro_byte_array = read_only_byte_array(byte_arr, None)
        try:
            mut_b_arr = self.deserializer.decode(ro_byte_array, None)
            ret = bytearray(mut_b_arr.get_max_len())
            mut_b_arr.get_data_copy(ret)
            return ret
        except Exception as e:
            raise AwsSchemaRegistryException(e)

    def decode_schema(self, byte_arr: bytes) -> GlueSchemaRegistrySchema:
        """
        Decodes the encoded byte array and returns the schema used to encode.
        :param byte_arr: Encoded byte array
        :return: Decoded Schema
        :raises AwsSchemaRegistryException in case of errors.
        """
        validate_byte_types('byte_arr', byte_arr)
        ro_byte_array = read_only_byte_array(byte_arr, None)
        try:
            gsr_schema = self.deserializer.decode_schema(ro_byte_array, None)
        except Exception as e:
            raise AwsSchemaRegistryException(e)

        schema = GlueSchemaRegistrySchema(
            gsr_schema.get_schema_name(),
            gsr_schema.get_schema_def(),
            gsr_schema.get_data_format(),
        )
        return schema

    def can_decode(self, byte_arr: bytes) -> bool:
        """
        Determines if the given byte_arr can be decoded using this de-serializer.
        :param byte_arr: Encoded byte array
        :return: True / False
        :raises AwsSchemaRegistryException in case of errors.
        """
        validate_byte_types('byte_arr', byte_arr)
        ro_byte_array = read_only_byte_array(byte_arr, None)
        try:
            return self.deserializer.can_decode(ro_byte_array, None)
        except Exception as e:
            raise AwsSchemaRegistryException(e)


def validate_type(name, param, *types):
    if type(param) not in types:
        raise ValueError("{} should be one of types: {}".format(name, types))


def validate_byte_types(name, param):
    validate_type(name, param, bytes, bytearray)
    param_len = len(param)
    if param_len < 1:
        raise ValueError("byte_array cannot be of length {}".format(param_len))
