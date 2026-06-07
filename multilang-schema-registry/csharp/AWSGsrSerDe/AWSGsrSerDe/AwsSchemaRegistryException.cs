using System;

namespace AWSGsrSerDe
{
    /// <summary>
    /// Represents exceptions thrown from Serializer / Deserializer methods.
    /// </summary>
    public class AwsSchemaRegistryException: Exception
    {
        public AwsSchemaRegistryException(string message) : base(message)
        {
        }

        public AwsSchemaRegistryException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}