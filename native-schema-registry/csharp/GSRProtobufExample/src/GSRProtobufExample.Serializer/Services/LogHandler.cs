using KafkaFlow;
using Microsoft.Extensions.Logging;

namespace GSRProtobufExample.Serializer.Services;

public class LogHandler : ILogHandler
{
    private readonly ILogger<LogHandler> _logger;

    public LogHandler(ILogger<LogHandler> logger)
    {
        _logger = logger;
    }

    public void Error(string message, Exception ex, object data)
    {
        _logger.LogError(ex, "KafkaFlow Error: {Message}. Data: {@Data}", message, data);
    }

    public void Info(string message, object data)
    {
        _logger.LogInformation("KafkaFlow Info: {Message}. Data: {@Data}", message, data);
    }

    public void Warning(string message, Exception ex, object data)
    {
        _logger.LogWarning(ex, "KafkaFlow Warning: {Message}. Data: {@Data}", message, data);
    }

    public void Verbose(string message, object data)
    {
        _logger.LogTrace("KafkaFlow Verbose: {Message}. Data: {@Data}", message, data);
    }
}
