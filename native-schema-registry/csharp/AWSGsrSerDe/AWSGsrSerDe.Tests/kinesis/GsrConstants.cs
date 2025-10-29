namespace AWSGsrSerDe.Tests.Kinesis
{
    public static class GlueSchemaRegistryConstants
    {
        public const string JsonObjectType = "JSON_OBJECT_TYPE";
    }

    public class GlueSchemaRegistryDataFormatConfiguration
    {
        public string AvroRecordType { get; set; }
    }
}
