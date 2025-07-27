using KafkaFlow;
using KafkaFlow.TypedHandler;
using KafkaFlowProtobufGsrDemo.Messages;
using Microsoft.Extensions.Logging;

namespace KafkaFlowProtobufGsrDemo.Consumer.MessageHandlers;

public class AnalyticsHandler : IMessageHandler<Analytics>
{
    private readonly ILogger<AnalyticsHandler> _logger;

    public AnalyticsHandler(ILogger<AnalyticsHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, Analytics message)
    {
        _logger.LogInformation("Received Analytics: {Id} - User: {UserId} - Event: {EventName} - Value: {Value} - Properties: {Properties} - Session: {SessionId}",
            message.Id,
            message.UserId,
            message.EventName,
            message.Value,
            string.Join(", ", message.Properties?.Select(p => $"{p.Key}={p.Value}") ?? []),
            message.SessionId);

        return Task.CompletedTask;
    }
}
