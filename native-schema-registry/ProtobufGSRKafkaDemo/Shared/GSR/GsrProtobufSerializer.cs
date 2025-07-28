using AWSGsrSerDe.serializer;
using Google.Protobuf;
using KafkaFlow;

namespace ProtobufGSRKafkaDemo.GSR;

public class GsrProtobufSerializer<T> : ISerializer
    where T : class, IMessage<T>, new()
{
    private readonly GlueSchemaRegistryKafkaSerializer _gsrSerializer;

    public GsrProtobufSerializer(string configPath)
    {
        _gsrSerializer = new GlueSchemaRegistryKafkaSerializer(configPath);
    }

    public byte[] Serialize(object message, ISerializerContext context)
    {
        var protobufMessage = message as T;
        if (protobufMessage == null)
        {
            throw new InvalidOperationException($"Message is not of type {typeof(T).Name}");
        }

        return _gsrSerializer.Serialize(protobufMessage, context.Topic);
    }

    public async Task SerializeAsync(object message, Stream output, ISerializerContext context)
    {
        var serialized = Serialize(message, context);
        await output.WriteAsync(serialized);
    }

    public void Dispose()
    {
        // GlueSchemaRegistryKafkaSerializer doesn't implement IDisposable
    }
}