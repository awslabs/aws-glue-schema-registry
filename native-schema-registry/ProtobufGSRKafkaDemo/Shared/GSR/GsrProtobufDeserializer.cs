using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.common;
using Google.Protobuf;
using KafkaFlow;

namespace ProtobufGSRKafkaDemo.GSR;

public class GsrProtobufDeserializer<T> : IDeserializer
    where T : class, IMessage<T>, new()
{
    private readonly GlueSchemaRegistryKafkaDeserializer _gsrDeserializer;

    public GsrProtobufDeserializer(string configPath)
    {
        try
        {
            Console.WriteLine($"[GSR-{typeof(T).Name}] Initializing deserializer with config: {configPath}");
            var dataConfig = new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, new T().Descriptor }
            });
            
            _gsrDeserializer = new GlueSchemaRegistryKafkaDeserializer(configPath, dataConfig);
            Console.WriteLine($"[GSR-{typeof(T).Name}] ✅ Deserializer initialized successfully");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GSR-{typeof(T).Name}] ❌ Deserializer initialization failed: {ex.Message}");
            throw;
        }
    }

    public object Deserialize(ReadOnlySpan<byte> data, Type type, ISerializerContext context)
    {
        try
        {
            Console.WriteLine($"[GSR-{typeof(T).Name}] Attempting to deserialize {data.Length} bytes from topic {context.Topic}");
            var result = _gsrDeserializer.Deserialize(context.Topic, data.ToArray());
            Console.WriteLine($"[GSR-{typeof(T).Name}] ✅ Deserialization successful: {result}");
            return result;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[GSR-{typeof(T).Name}] ❌ Deserialization failed: {ex.Message}");
            Console.WriteLine($"[GSR-{typeof(T).Name}] Stack trace: {ex.StackTrace}");
            throw;
        }
    }

    public async Task<object> DeserializeAsync(Stream input, Type type, ISerializerContext context)
    {
        var data = new byte[input.Length];
        await input.ReadAsync(data);
        return Deserialize(data, type, context);
    }

    public void Dispose()
    {
        // GlueSchemaRegistryKafkaDeserializer doesn't implement IDisposable
    }
}