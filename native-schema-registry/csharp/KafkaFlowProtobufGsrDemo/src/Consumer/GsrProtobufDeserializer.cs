using AWS.GlueSchemaRegistry.SerDe;
using Google.Protobuf;
using KafkaFlow;

namespace KafkaFlowProtobufGsrDemo.Consumer;

public class GsrProtobufDeserializer<T> : IDeserializer
    where T : class, IMessage<T>, new()
{
    private readonly GlueSchemaRegistryKafkaDeserializer _gsrDeserializer;

    public GsrProtobufDeserializer()
    {
        var config = new GlueSchemaRegistryConfiguration
        {
            Region = "us-west-2",
            RegistryName = "KafkaFlow-Demo",
            DataFormat = DataFormat.PROTOBUF
        };
        
        _gsrDeserializer = new GlueSchemaRegistryKafkaDeserializer(config);
    }

    public object Deserialize(ReadOnlySpan<byte> data, Type type, ISerializerContext context)
    {
        var deserializedBytes = _gsrDeserializer.Deserialize(context.Topic, data.ToArray());
        
        var parser = MessageParser.CreateParser<T>(() => new T());
        return parser.ParseFrom(deserializedBytes);
    }

    public void Dispose()
    {
        _gsrDeserializer?.Dispose();
    }
}
