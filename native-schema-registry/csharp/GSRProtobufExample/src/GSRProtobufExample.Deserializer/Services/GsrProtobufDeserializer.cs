using Amazon.GlueSchemaRegistry.SerDe;
using Example.V1;
using Google.Protobuf;
using KafkaFlow;

namespace GSRProtobufExample.Deserializer.Services;

public class GsrProtobufDeserializer : IDeserializer
{
    private readonly GlueSchemaRegistryKafkaDeserializer _gsrDeserializer;
    private readonly Dictionary<string, Func<byte[], IMessage>> _deserializerMap;

    public GsrProtobufDeserializer()
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

        _gsrDeserializer = new GlueSchemaRegistryKafkaDeserializer(config);

        // Map schema names to protobuf deserializers
        _deserializerMap = new Dictionary<string, Func<byte[], IMessage>>
        {
            { "example.v1.User", data => User.Parser.ParseFrom(data) },
            { "example.v1.Product", data => Product.Parser.ParseFrom(data) },
            { "example.v1.Order", data => Order.Parser.ParseFrom(data) },
            { "example.v1.Event", data => Event.Parser.ParseFrom(data) },
            { "example.v1.Company", data => Company.Parser.ParseFrom(data) }
        };
    }

    public object Deserialize(byte[] data, Type type)
    {
        var (schemaName, deserializedData) = _gsrDeserializer.Deserialize(data);
        
        if (_deserializerMap.TryGetValue(schemaName, out var deserializer))
        {
            return deserializer(deserializedData);
        }

        throw new ArgumentException($"No deserializer found for schema: {schemaName}");
    }

    public byte[] Serialize(object message)
    {
        // This deserializer is for inbound messages only
        throw new NotImplementedException("This deserializer is for inbound messages only");
    }

    public async Task<object> DeserializeAsync(byte[] data, Type type)
    {
        return await Task.Run(() => Deserialize(data, type));
    }

    public async Task<byte[]> SerializeAsync(object message)
    {
        return await Task.Run(() => Serialize(message));
    }
}
