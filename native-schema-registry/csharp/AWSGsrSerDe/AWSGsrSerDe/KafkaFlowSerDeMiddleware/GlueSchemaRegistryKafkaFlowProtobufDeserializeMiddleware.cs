using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.common;
using Google.Protobuf;
using KafkaFlow;

namespace AWSGsrSerDe.KafkaFlow
{
    /// <summary>
    /// KafkaFlow middleware that DESERIALIZES byte[] -> T using AWS Glue SR.
    /// Register it with MiddlewareLifetime.Worker for one instance per worker.
    /// </summary>
    public sealed class GlueSchemaRegistryKafkaFlowProtobufDeserializeMiddleware<T> : IMessageMiddleware
        where T : class, IMessage<T>, new()
    {
        private readonly GlueSchemaRegistryKafkaDeserializer _gsr;

        public GlueSchemaRegistryKafkaFlowProtobufDeserializeMiddleware(string configPath)
        {
            try
            {
                var dataConfig = new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
                {
                    { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, new T().Descriptor }
                });

                _gsr = new GlueSchemaRegistryKafkaDeserializer(configPath, dataConfig);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to initialize GlueSchemaRegistry deserializer for {typeof(T).Name}: {ex.Message}", ex);
            }
        }

        public async Task Invoke(IMessageContext context, MiddlewareDelegate next)
        {
            if (context is null) throw new ArgumentNullException(nameof(context));

            // Expect raw bytes coming from Kafka
            if (context.Message.Value is not byte[] payload)
                throw new InvalidOperationException(
                    $"Expected byte[] payload before deserialization, got {context.Message.Value?.GetType().FullName ?? "null"}");

            // Topic to assist SR lookup / subject resolution
            var topic = context.ConsumerContext?.Topic
                        ?? throw new InvalidOperationException("Topic not available in ConsumerContext");

            T value;
            try
            {
                // Use the deserializer to get the typed object
                value = (T)_gsr.Deserialize(topic, payload);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to deserialize {typeof(T).Name} from topic '{topic}': {ex.Message}", ex);
            }

            // Create a new message with the deserialized value and continue to next middleware
            var transformedContext = context.SetMessage(context.Message.Key, value);

            await next(transformedContext);
        }
    }
}
