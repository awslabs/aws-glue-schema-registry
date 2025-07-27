using Example.V1;
using KafkaFlow;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace GSRProtobufExample.Deserializer.Services;

public class UserHandler : IMessageHandler<User>
{
    private readonly ILogger<UserHandler> _logger;

    public UserHandler(ILogger<UserHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, User message)
    {
        _logger.LogInformation("Received User message: {@User}", new
        {
            message.Id,
            message.Name,
            message.Email,
            message.Age
        });

        return Task.CompletedTask;
    }
}

public class ProductHandler : IMessageHandler<Product>
{
    private readonly ILogger<ProductHandler> _logger;

    public ProductHandler(ILogger<ProductHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, Product message)
    {
        _logger.LogInformation("Received Product message: {@Product}", new
        {
            message.Id,
            message.Name,
            Price = new
            {
                message.Price?.Amount,
                message.Price?.Currency
            }
        });

        return Task.CompletedTask;
    }
}

public class OrderHandler : IMessageHandler<Order>
{
    private readonly ILogger<OrderHandler> _logger;

    public OrderHandler(ILogger<OrderHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, Order message)
    {
        var items = message.Items.Select(item => new
        {
            item.ProductId,
            item.Quantity,
            item.Price
        }).ToList();

        _logger.LogInformation("Received Order message: {@Order}", new
        {
            message.Id,
            message.CustomerId,
            message.Total,
            ItemCount = message.Items.Count,
            Items = items
        });

        return Task.CompletedTask;
    }
}

public class EventHandler : IMessageHandler<Event>
{
    private readonly ILogger<EventHandler> _logger;

    public EventHandler(ILogger<EventHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, Event message)
    {
        var timestamp = DateTimeOffset.FromUnixTimeSeconds(message.Timestamp);

        _logger.LogInformation("Received Event message: {@Event}", new
        {
            message.Id,
            Type = message.Type.ToString(),
            message.UserId,
            Timestamp = timestamp.ToString("yyyy-MM-dd HH:mm:ss UTC")
        });

        return Task.CompletedTask;
    }
}

public class CompanyHandler : IMessageHandler<Company>
{
    private readonly ILogger<CompanyHandler> _logger;

    public CompanyHandler(ILogger<CompanyHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(IMessageContext context, Company message)
    {
        var employees = message.Employees.Select(emp => new
        {
            emp.Id,
            emp.Name,
            emp.Email,
            emp.Age
        }).ToList();

        var metadata = message.Metadata.ToDictionary(kv => kv.Key, kv => kv.Value);

        var contactMethod = message.ContactMethodCase switch
        {
            Company.ContactMethodOneofCase.Email => $"Email: {message.Email}",
            Company.ContactMethodOneofCase.Phone => $"Phone: {message.Phone}",
            _ => "No contact method specified"
        };

        _logger.LogInformation("Received Company message: {@Company}", new
        {
            message.Id,
            message.Name,
            Address = message.Address != null ? new
            {
                message.Address.Street,
                message.Address.City,
                message.Address.State,
                message.Address.Country,
                message.Address.PostalCode
            } : null,
            EmployeeCount = message.Employees.Count,
            Employees = employees,
            ContactMethod = contactMethod,
            Metadata = metadata
        });

        return Task.CompletedTask;
    }
}
