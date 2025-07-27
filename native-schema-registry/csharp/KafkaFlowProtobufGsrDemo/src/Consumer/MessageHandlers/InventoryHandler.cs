using KafkaFlow;
using KafkaFlow.TypedHandler;
using KafkaFlowProtobufGsrDemo.Messages;
using Microsoft.Extensions.Logging;

namespace KafkaFlowProtobufGsrDemo.Consumer.MessageHandlers;

public class InventoryHandler : IMessageHandler<Inventory>
{
    private readonly ILogger<InventoryHandler> _logger;

    public InventoryHandler(ILogger<InventoryHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, Inventory message)
    {
        _logger.LogInformation("Received Inventory: {Sku} - {Name} - Quantity: {Quantity} - Price: {Price} {Currency} - Status: {Status} - Location: {Location} - Supplier: {SupplierId}",
            message.Sku,
            message.Name,
            message.Quantity,
            message.Price,
            message.Currency,
            message.Status,
            message.Location,
            message.SupplierId);

        return Task.CompletedTask;
    }
}
