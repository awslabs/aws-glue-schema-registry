using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.common;
using Google.Protobuf;
using KafkaFlow;
using Example.V1;

namespace GSRProtobufExample.Deserializer.Services;

public class GsrProtobufDeserializer : IDeserializer
{
    private readonly GlueSchemaRegistryKafkaDeserializer _gsrDeserializer;

    public GsrProtobufDeserializer()
    {
        var configPath = "config.properties";
        _gsrDeserializer = new GlueSchemaRegistryKafkaDeserializer(configPath);
    }

    public async Task<object> DeserializeAsync(Stream input, Type type, ISerializerContext context)
    {
        var data = new byte[input.Length];
        await input.ReadAsync(data, 0, data.Length);
        
        return await Task.Run(() => Deserialize(context.Topic, data, type));
    }

    private object Deserialize(string topic, byte[] data, Type targetType)
    {
        if (data == null) return null;

        var result = _gsrDeserializer.Deserialize(topic, data);
        
        // The deserializer returns the correct protobuf message type
        // based on the schema registered in GSR
        return result;
    }
}
