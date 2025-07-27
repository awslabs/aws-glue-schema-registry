using AWS.GlueSchemaRegistry.SerDe;
using Google.Protobuf;
using KafkaFlow;

namespace KafkaFlowProtobufGsrDemo.Producer;

public class GsrProtobufSerializer<T> : ISerializer
    where T : class, IMessage<T>, new()
{
    private readonly GlueSchemaRegistryKafkaSerializer _gsrSerializer;

    public GsrProtobufSerializer()
    {
        var config = new GlueSchemaRegistryConfiguration
        {
            Region = "us-west-2",
            RegistryName = "KafkaFlow-Demo",
            SchemaAutoRegistrationEnabled = true,
            DataFormat = DataFormat.PROTOBUF
        };
        
        _gsrSerializer = new GlueSchemaRegistryKafkaSerializer(config);
    }

    public byte[] Serialize(object message, ISerializerContext context)
    {
        var protobufMessage = message as T;
        if (protobufMessage == null)
        {
            throw new InvalidOperationException($"Message is not of type {typeof(T).Name}");
        }

        var messageBytes = protobufMessage.ToByteArray();
        return _gsrSerializer.Serialize(context.Topic, messageBytes, typeof(T).FullName);
    }

    public void Dispose()
    {
        _gsrSerializer?.Dispose();
    }
}
