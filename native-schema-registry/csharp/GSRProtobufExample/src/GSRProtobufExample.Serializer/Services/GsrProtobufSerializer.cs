using AWSGsrSerDe.serializer;
using Example.V1;
using Google.Protobuf;
using KafkaFlow;

namespace GSRProtobufExample.Serializer.Services;

public class GsrProtobufSerializer : ISerializer
{
    private readonly GlueSchemaRegistryKafkaSerializer _gsrSerializer;
    private readonly Dictionary<Type, Func<IMessage, string>> _schemaNameMap;

    public GsrProtobufSerializer()
    {
        // Configure GSR with properties file path
        var configPath = "config.properties";
        _gsrSerializer = new GlueSchemaRegistryKafkaSerializer(configPath);

        // Map protobuf types to schema names
        _schemaNameMap = new Dictionary<Type, Func<IMessage, string>>
        {
            { typeof(User), msg => "example.v1.User" },
            { typeof(Product), msg => "example.v1.Product" },
            { typeof(Order), msg => "example.v1.Order" },
            { typeof(Event), msg => "example.v1.Event" },
            { typeof(Company), msg => "example.v1.Company" }
        };
    }

    public async Task SerializeAsync(object message, Stream output, ISerializerContext context)
    {
        var serializedData = await Task.Run(() => Serialize(message, context.Topic));
        await output.WriteAsync(serializedData, 0, serializedData.Length);
    }

    private byte[] Serialize(object message, string topic)
    {
        if (message is IMessage protobufMessage)
        {
            return _gsrSerializer.Serialize(protobufMessage, topic);
        }

        throw new ArgumentException($"Message must be a protobuf IMessage, but was {message?.GetType()}");
    }
}
