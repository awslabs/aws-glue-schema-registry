using KafkaFlow;
using KafkaFlow.TypedHandler;
using KafkaFlowProtobufGsrDemo.Messages;
using Microsoft.Extensions.Logging;

namespace KafkaFlowProtobufGsrDemo.Consumer.MessageHandlers;

public class NotificationHandler : IMessageHandler<Notification>
{
    private readonly ILogger<NotificationHandler> _logger;

    public NotificationHandler(ILogger<NotificationHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, Notification message)
    {
        _logger.LogInformation("Received Notification: {Id} - {Title} - Recipient: {RecipientId} - Type: {Type} - Priority: {Priority} - Channel: {Channel} - Status: {Status}",
            message.Id,
            message.Title,
            message.RecipientId,
            message.Type,
            message.Priority,
            message.Channel,
            message.Status);

        return Task.CompletedTask;
    }
}
