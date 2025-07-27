using KafkaFlow;
using KafkaFlow.TypedHandler;
using KafkaFlowProtobufGsrDemo.Messages;
using Microsoft.Extensions.Logging;

namespace KafkaFlowProtobufGsrDemo.Consumer.MessageHandlers;

public class TransactionHandler : IMessageHandler<Transaction>
{
    private readonly ILogger<TransactionHandler> _logger;

    public TransactionHandler(ILogger<TransactionHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, Transaction message)
    {
        _logger.LogInformation("Received Transaction: {Id} - Customer: {CustomerId} - Amount: {Amount} {Currency} - Type: {Type} - Status: {Status} - Payment Method: {PaymentMethod}",
            message.Id,
            message.CustomerId,
            message.Amount,
            message.Currency,
            message.Type,
            message.Status,
            message.PaymentMethod);

        return Task.CompletedTask;
    }
}
