#pragma warning disable CS1591
namespace AWSGsrSerDe.common
{
    /// <summary>
    /// Constants and enums
    /// </summary>
    public class GlueSchemaRegistryConstants
    {
        public const string AvroRecordType = "avroRecordType";
        public const string CacheItemExpirationTime = "cacheItemExpirationTime";

        // TODO: need to expose it from Java to avoid code duplication
        public enum DataFormat
        {
            UNKNOWN,
            AVRO,
            JSON,
            PROTOBUF
        }
    }
}
