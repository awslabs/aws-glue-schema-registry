using KafkaFlow;
using KafkaFlow.TypedHandler;
using KafkaFlowProtobufGsrDemo.Messages;
using Microsoft.Extensions.Logging;

namespace KafkaFlowProtobufGsrDemo.Consumer.MessageHandlers;

public class CustomerHandler : IMessageHandler<Customer>
{
    private readonly ILogger<CustomerHandler> _logger;

    public CustomerHandler(ILogger<CustomerHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, Customer message)
    {
        _logger.LogInformation("Received Customer: {Id} - {Name} ({Email}) - Status: {Status} - Address: {Street}, {City}, {State} {ZipCode}",
            message.Id,
            message.Name,
            message.Email,
            message.Status,
            message.Address?.Street,
            message.Address?.City,
            message.Address?.State,
            message.Address?.ZipCode);

        return Task.CompletedTask;
    }
}
