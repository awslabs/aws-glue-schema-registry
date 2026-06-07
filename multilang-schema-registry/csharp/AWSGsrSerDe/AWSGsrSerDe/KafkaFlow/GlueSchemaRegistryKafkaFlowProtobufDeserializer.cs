using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.common;
using Google.Protobuf;
using KafkaFlow;

namespace AWSGsrSerDe.KafkaFlow;

public class GlueSchemaRegistryKafkaFlowProtobufDeserializer<T> : IDeserializer
    where T : class, IMessage<T>, new()
{
    private readonly GlueSchemaRegistryKafkaDeserializer _gsrDeserializer;

    public GlueSchemaRegistryKafkaFlowProtobufDeserializer(string configPath)
    {
        try
        {
            var dataConfig = new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, new T().Descriptor }
            });

            _gsrDeserializer = new GlueSchemaRegistryKafkaDeserializer(configPath, dataConfig);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to initialize GlueSchemaRegistry KafkaFlow deserializer for {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public object Deserialize(ReadOnlySpan<byte> data, Type type, ISerializerContext context)
    {
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        try
        {
            return _gsrDeserializer.Deserialize(context.Topic, data.ToArray());
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to deserialize {typeof(T).Name} from topic {context.Topic}: {ex.Message}", ex);
        }
    }

    public async Task<object> DeserializeAsync(Stream input, Type type, ISerializerContext context)
    {
        if (input == null)
            throw new ArgumentNullException(nameof(input));
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        var data = new byte[input.Length];
        await input.ReadAsync(data);
        return Deserialize(data, type, context);
    }

    // Dispose method is not needed as .NET GC will clean this object and the underlying GlueSchemaRegistryKafkaDeserializer does not implement IDisposable.
    // If it did, we would implement IDisposable and call Dispose on the deserializer in the Dispose method.
}
