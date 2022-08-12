namespace AWSGsrSerDe.common
{
    /// <summary>
    /// Interface for Schema Naming strategy
    /// </summary>
    public interface ISchemaNameStrategy
    {
        /// <summary>
        /// Generates the schema name
        /// </summary>
        /// <param name="data">message object to serialize</param>
        /// <param name="topic">Kafka topic name</param>
        /// <returns>schema name</returns>
        string GetSchemaName(object data, string topic);
    }

    class DefaultSchemaNameStrategy : ISchemaNameStrategy
    {
        public string GetSchemaName(object data, string topic)
        {
            return topic;
        }
    }
}