using System.Collections.Generic;

namespace AWSGsrSerDe.common
{
    /// <summary>
    /// Glue Schema Registry Configuration entries.
    /// </summary>
    public class GlueSchemaRegistryConfiguration
    {
        /// <summary>
        /// 
        /// </summary>
        public AvroRecordType AvroRecordType { get; private set; }

        /// <summary>
        /// Build Configuration object from config elements
        /// </summary>
        /// <param name="configs"></param>
        public GlueSchemaRegistryConfiguration(Dictionary<string, dynamic> configs)
        {
            BuildConfigs(configs);
        }

        private void BuildConfigs(Dictionary<string, dynamic> configs)
        {
            ValidateAndSetAvroRecordType(configs);
        }

        private void ValidateAndSetAvroRecordType(Dictionary<string, dynamic> configs)
        {
            if (configs.ContainsKey(GlueSchemaRegistryConstants.AvroRecordType))
            {
                AvroRecordType = configs[GlueSchemaRegistryConstants.AvroRecordType];
            }
        }
        
    }
}
