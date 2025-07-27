using Example.V1;
using Google.Protobuf.WellKnownTypes;
using KafkaFlow;
using Microsoft.Extensions.Logging;

namespace GSRProtobufExample.Serializer.Services;

public class MessagePublisher
{
    private readonly ILogger<MessagePublisher> _logger;
    private readonly IMessageProducer<UserProducer> _userProducer;
    private readonly IMessageProducer<ProductProducer> _productProducer;
    private readonly IMessageProducer<OrderProducer> _orderProducer;
    private readonly IMessageProducer<EventProducer> _eventProducer;
    private readonly IMessageProducer<CompanyProducer> _companyProducer;

    public MessagePublisher(
        ILogger<MessagePublisher> logger,
        IMessageProducer<UserProducer> userProducer,
        IMessageProducer<ProductProducer> productProducer,
        IMessageProducer<OrderProducer> orderProducer,
        IMessageProducer<EventProducer> eventProducer,
        IMessageProducer<CompanyProducer> companyProducer)
    {
        _logger = logger;
        _userProducer = userProducer;
        _productProducer = productProducer;
        _orderProducer = orderProducer;
        _eventProducer = eventProducer;
        _companyProducer = companyProducer;
    }

    public async Task PublishSampleMessagesAsync()
    {
        _logger.LogInformation("Publishing sample messages...");

        // Publish User message (simple message)
        await PublishUserAsync();
        
        // Publish Product message (nested message)
        await PublishProductAsync();
        
        // Publish Order message (repeated fields)
        await PublishOrderAsync();
        
        // Publish Event message (with enum)
        await PublishEventAsync();
        
        // Publish Company message (complex with imports, maps, oneof)
        await PublishCompanyAsync();

        _logger.LogInformation("All sample messages published successfully!");
    }

    private async Task PublishUserAsync()
    {
        var user = new User
        {
            Id = "user-123",
            Name = "John Doe",
            Email = "john.doe@example.com",
            Age = 30
        };

        _logger.LogInformation("Publishing User: {UserId}", user.Id);
        await _userProducer.ProduceAsync("user-123", user);
    }

    private async Task PublishProductAsync()
    {
        var product = new Product
        {
            Id = "product-456",
            Name = "Laptop Computer",
            Price = new Price
            {
                Amount = 999.99,
                Currency = "USD"
            }
        };

        _logger.LogInformation("Publishing Product: {ProductId}", product.Id);
        await _productProducer.ProduceAsync("product-456", product);
    }

    private async Task PublishOrderAsync()
    {
        var order = new Order
        {
            Id = "order-789",
            CustomerId = "user-123",
            Total = 1249.98
        };

        order.Items.Add(new OrderItem
        {
            ProductId = "product-456",
            Quantity = 1,
            Price = 999.99
        });

        order.Items.Add(new OrderItem
        {
            ProductId = "product-789",
            Quantity = 2,
            Price = 124.99
        });

        _logger.LogInformation("Publishing Order: {OrderId} with {ItemCount} items", 
            order.Id, order.Items.Count);
        await _orderProducer.ProduceAsync("order-789", order);
    }

    private async Task PublishEventAsync()
    {
        var loginEvent = new Event
        {
            Id = "event-001",
            Type = EventType.Login,
            UserId = "user-123",
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds()
        };

        _logger.LogInformation("Publishing Event: {EventId} of type {EventType}", 
            loginEvent.Id, loginEvent.Type);
        await _eventProducer.ProduceAsync("event-001", loginEvent);
    }

    private async Task PublishCompanyAsync()
    {
        var company = new Company
        {
            Id = "company-999",
            Name = "Tech Innovators Inc",
            Address = new Address
            {
                Street = "123 Innovation Drive",
                City = "San Francisco",
                State = "CA",
                Country = "USA",
                PostalCode = "94105"
            },
            Email = "contact@techinnovators.com" // Using oneof contact_method
        };

        // Add employees (imported User type)
        company.Employees.Add(new User
        {
            Id = "user-123",
            Name = "John Doe",
            Email = "john.doe@techinnovators.com",
            Age = 30
        });

        company.Employees.Add(new User
        {
            Id = "user-456",
            Name = "Jane Smith",
            Email = "jane.smith@techinnovators.com",
            Age = 28
        });

        // Add metadata using map
        company.Metadata.Add("founded", "2010");
        company.Metadata.Add("industry", "Technology");
        company.Metadata.Add("size", "500-1000");

        _logger.LogInformation("Publishing Company: {CompanyId} with {EmployeeCount} employees", 
            company.Id, company.Employees.Count);
        await _companyProducer.ProduceAsync("company-999", company);
    }
}

// Producer marker classes for DI
public class UserProducer { }
public class ProductProducer { }
public class OrderProducer { }
public class EventProducer { }
public class CompanyProducer { }
