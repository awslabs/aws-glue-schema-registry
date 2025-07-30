using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.common;
using Google.Protobuf;
using KafkaFlow;

namespace AWSGsrSerDe.KafkaFlow;

public class GsrKafkaFlowProtobufDeserializer<T> : IDeserializer
    where T : class, IMessage<T>, new()
{
    private readonly GlueSchemaRegistryKafkaDeserializer _gsrDeserializer;

    public GsrKafkaFlowProtobufDeserializer(string configPath)
    {
        try
        {
            var dataConfig = new GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.ProtobufMessageDescriptor, new T().Descriptor }
            });
            
            _gsrDeserializer = new GlueSchemaRegistryKafkaDeserializer(configPath, dataConfig);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to initialize GSR KafkaFlow deserializer for {typeof(T).Name}: {ex.Message}", ex);
        }
    }

    public object Deserialize(ReadOnlySpan<byte> data, Type type, ISerializerContext context)
    {
        try
        {
            return _gsrDeserializer.Deserialize(context.Topic, data.ToArray());
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to deserialize {typeof(T).Name} from topic {context.Topic}: {ex.Message}", ex);
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
        // The underlying GlueSchemaRegistryDeserializer implements IDisposable
        // but GlueSchemaRegistryKafkaDeserializer doesn't expose it, so no disposal needed here
    }
}
