using Amazon.GlueSchemaRegistry.SerDe;
using Google.Protobuf;
using KafkaFlow;
using System.Text;

namespace GSRProtobufExample.Serializer.Services;

public class GsrProtobufSerializer : ISerializer
{
    private readonly GlueSchemaRegistryKafkaSerializer _gsrSerializer;

    public GsrProtobufSerializer()
    {
        // Configure GSR for Protobuf
        var config = new Dictionary<string, object>
        {
            {"schemaregistryname", "gsr-protobuf-example"},
            {"region", "us-east-1"},
            {"dataformat", "PROTOBUF"},
            {"compressiontype", "NONE"},
            {"cachettl", 86400000}, // 24 hours in milliseconds
            {"cachesize", 200}
        };

        _gsrSerializer = new GlueSchemaRegistryKafkaSerializer(config);
    }

    public byte[] Serialize(object message)
    {
        if (message is IMessage protobufMessage)
        {
            var schemaName = protobufMessage.Descriptor.FullName;
            return _gsrSerializer.Serialize(schemaName, protobufMessage.ToByteArray());
        }

        throw new ArgumentException($"Message must be a protobuf IMessage, but was {message?.GetType()}");
    }

    public object Deserialize(byte[] data, Type type)
    {
        // This serializer is for outbound messages only
        throw new NotImplementedException("This serializer is for outbound messages only");
    }

    public async Task<byte[]> SerializeAsync(object message)
    {
        return await Task.Run(() => Serialize(message));
    }

    public async Task<object> DeserializeAsync(byte[] data, Type type)
    {
        return await Task.Run(() => Deserialize(data, type));
    }
}
