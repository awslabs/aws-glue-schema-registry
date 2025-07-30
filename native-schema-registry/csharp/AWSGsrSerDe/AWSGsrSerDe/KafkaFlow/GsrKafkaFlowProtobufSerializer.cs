using System;
using System.IO;
using System.Threading.Tasks;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.common;
using Google.Protobuf;
using KafkaFlow;

namespace AWSGsrSerDe.KafkaFlow;

public class GsrKafkaFlowProtobufSerializer<T> : ISerializer
    where T : class, IMessage<T>, new()
{
    private readonly GlueSchemaRegistryKafkaSerializer _gsrSerializer;

    public GsrKafkaFlowProtobufSerializer(string configPath)
    {
        try
        {
            _gsrSerializer = new GlueSchemaRegistryKafkaSerializer(configPath);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to initialize GSR KafkaFlow serializer for {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public byte[] Serialize(object message, ISerializerContext context)
    {
        try
        {
            return _gsrSerializer.Serialize(message, context.Topic);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to serialize {typeof(T).Name} to topic {context.Topic}: {ex.Message}", ex);
        }
    }

    public async Task SerializeAsync(object message, Stream output, ISerializerContext context)
    {
        var data = Serialize(message, context);
        await output.WriteAsync(data);
    }

    public void Dispose()
    {
        // GlueSchemaRegistryKafkaSerializer doesn't implement IDisposable
    }
}
