using System;
using System.IO;
using System.Threading.Tasks;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.common;
using Google.Protobuf;
using KafkaFlow;

namespace AWSGsrSerDe.KafkaFlow;

public class GlueSchemaRegistryKafkaFlowProtobufSerializer<T> : ISerializer
    where T : class, IMessage<T>, new()
{
    private readonly GlueSchemaRegistryKafkaSerializer _gsrSerializer;
    private const string PROTOBUF_DATA_FORMAT = "PROTOBUF";

    public GlueSchemaRegistryKafkaFlowProtobufSerializer(string configPath)
    {
        try
        {
            _gsrSerializer = new GlueSchemaRegistryKafkaSerializer(configPath);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to initialize GlueSchemaRegistry KafkaFlow serializer for {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public byte[] Serialize(object message, ISerializerContext context)
    {
        if (message == null)
            throw new ArgumentNullException(nameof(message));
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        try
        {
            return _gsrSerializer.Serialize(message, context.Topic, PROTOBUF_DATA_FORMAT);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to serialize {typeof(T).Name} to topic {context.Topic}: {ex.Message}", ex);
        }
    }

    public async Task SerializeAsync(object message, Stream output, ISerializerContext context)
    {
        if (message == null)
            throw new ArgumentNullException(nameof(message));
        if (output == null)
            throw new ArgumentNullException(nameof(output));
        if (context == null)
            throw new ArgumentNullException(nameof(context));

        var data = Serialize(message, context);
        await output.WriteAsync(data);
    }

    // Dispose method is not needed as .NET GC will clean this object and the underlying GlueSchemaRegistryKafkaSerializer does not implement IDisposable.
    // If it did, we would implement IDisposable and call Dispose on the serializer in the Dispose

}
