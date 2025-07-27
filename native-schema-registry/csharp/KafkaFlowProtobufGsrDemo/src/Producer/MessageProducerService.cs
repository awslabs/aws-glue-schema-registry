using KafkaFlow;
using KafkaFlow.Producers;
using KafkaFlowProtobufGsrDemo.Messages;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace KafkaFlowProtobufGsrDemo.Producer;

public class MessageProducerService : BackgroundService
{
    private readonly IProducerAccessor _producerAccessor;
    private readonly ILogger<MessageProducerService> _logger;

    public MessageProducerService(IProducerAccessor producerAccessor, ILogger<MessageProducerService> logger)
    {
        _producerAccessor = producerAccessor;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var random = new Random();
        var messageCount = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProduceCustomer(messageCount, random);
                await Task.Delay(1000, stoppingToken);

                await ProduceTransaction(messageCount, random);
                await Task.Delay(1000, stoppingToken);

                await ProduceInventory(messageCount, random);
                await Task.Delay(1000, stoppingToken);

                await ProduceNotification(messageCount, random);
                await Task.Delay(1000, stoppingToken);

                await ProduceAnalytics(messageCount, random);
                await Task.Delay(1000, stoppingToken);

                messageCount++;
                _logger.LogInformation("Produced message batch {MessageCount}", messageCount);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error producing messages");
                await Task.Delay(5000, stoppingToken);
            }
        }
    }

    private async Task ProduceCustomer(int messageCount, Random random)
    {
        var producer = _producerAccessor.GetProducer("customer-producer");
        
        var customer = new Customer
        {
            Id = $"CUST_{messageCount:000}",
            Name = $"Customer {messageCount}",
            Email = $"customer{messageCount}@example.com",
            Phone = $"+1-555-{random.Next(1000, 9999)}",
            CreatedTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            Status = CustomerStatus.Active,
            Address = new Address
            {
                Street = $"{random.Next(100, 999)} Main St",
                City = GetRandomCity(random),
                State = GetRandomState(random),
                ZipCode = $"{random.Next(10000, 99999)}",
                Country = "US"
            }
        };

        await producer.ProduceAsync(customer.Id, customer);
        _logger.LogInformation("Produced Customer: {Id}", customer.Id);
    }

    private async Task ProduceTransaction(int messageCount, Random random)
    {
        var producer = _producerAccessor.GetProducer("transaction-producer");
        
        var transaction = new Transaction
        {
            Id = $"TXN_{messageCount:000}",
            CustomerId = $"CUST_{random.Next(0, messageCount + 1):000}",
            Amount = random.NextDouble() * 1000,
            Currency = "USD",
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            Type = GetRandomTransactionType(random),
            Description = $"Transaction {messageCount}",
            MerchantId = $"MERCH_{random.Next(1, 100):000}",
            PaymentMethod = GetRandomPaymentMethod(random),
            Status = GetRandomTransactionStatus(random)
        };

        await producer.ProduceAsync(transaction.Id, transaction);
        _logger.LogInformation("Produced Transaction: {Id} - ${Amount:F2}", transaction.Id, transaction.Amount);
    }

    private async Task ProduceInventory(int messageCount, Random random)
    {
        var producer = _producerAccessor.GetProducer("inventory-producer");
        
        var inventory = new Inventory
        {
            Sku = $"SKU_{messageCount:000}",
            Name = $"Product {messageCount}",
            Description = $"Description for product {messageCount}",
            Quantity = random.Next(0, 1000),
            Price = random.NextDouble() * 500,
            Currency = "USD",
            Location = GetRandomLocation(random),
            Status = GetRandomInventoryStatus(random),
            LastUpdated = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            Category = GetRandomCategory(random),
            SupplierId = $"SUPP_{random.Next(1, 50):000}",
            ReorderLevel = random.Next(10, 50),
            MaxStockLevel = random.Next(500, 1500)
        };

        await producer.ProduceAsync(inventory.Sku, inventory);
        _logger.LogInformation("Produced Inventory: {Sku} - Qty: {Quantity}", inventory.Sku, inventory.Quantity);
    }

    private async Task ProduceNotification(int messageCount, Random random)
    {
        var producer = _producerAccessor.GetProducer("notification-producer");
        
        var notification = new Notification
        {
            Id = $"NOTIF_{messageCount:000}",
            RecipientId = $"USER_{random.Next(1, 100):000}",
            Type = GetRandomNotificationType(random),
            Title = $"Notification {messageCount}",
            Message = $"This is notification message {messageCount}",
            Priority = GetRandomNotificationPriority(random),
            Channel = GetRandomNotificationChannel(random),
            CreatedTimestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            ScheduledTimestamp = DateTimeOffset.UtcNow.AddMinutes(random.Next(1, 60)).ToUnixTimeSeconds(),
            Status = NotificationStatus.Pending,
            SourceSystem = "KafkaFlow-Demo"
        };

        notification.Metadata.Add("category", "demo");
        notification.Metadata.Add("environment", "development");

        await producer.ProduceAsync(notification.Id, notification);
        _logger.LogInformation("Produced Notification: {Id} - {Type}", notification.Id, notification.Type);
    }

    private async Task ProduceAnalytics(int messageCount, Random random)
    {
        var producer = _producerAccessor.GetProducer("analytics-producer");
        
        var analytics = new Analytics
        {
            EventId = $"EVENT_{messageCount:000}",
            UserId = $"USER_{random.Next(1, 100):000}",
            SessionId = $"SESSION_{random.Next(1000, 9999)}",
            EventType = GetRandomEventType(random),
            EventName = $"event_{messageCount}",
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
            DurationMs = random.NextDouble() * 5000,
            PageUrl = $"https://example.com/page/{messageCount}",
            ReferrerUrl = "https://google.com",
            UserAgent = new UserAgent
            {
                Raw = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                Browser = "Chrome",
                BrowserVersion = "120.0.0",
                Os = "Windows",
                OsVersion = "10",
                DeviceType = "Desktop"
            },
            Location = new Location
            {
                IpAddress = $"192.168.{random.Next(1, 255)}.{random.Next(1, 255)}",
                Country = "US",
                Region = GetRandomState(random),
                City = GetRandomCity(random),
                Latitude = random.NextDouble() * 180 - 90,
                Longitude = random.NextDouble() * 360 - 180,
                Timezone = "UTC"
            },
            DeviceInfo = new DeviceInfo
            {
                DeviceId = Guid.NewGuid().ToString(),
                Platform = "Web",
                AppVersion = "1.0.0",
                ScreenResolution = "1920x1080",
                Language = "en-US",
                IsMobile = false
            }
        };

        analytics.Properties.Add("custom_prop", $"value_{messageCount}");
        analytics.Properties.Add("test_flag", "true");

        await producer.ProduceAsync(analytics.EventId, analytics);
        _logger.LogInformation("Produced Analytics: {EventId} - {EventType}", analytics.EventId, analytics.EventType);
    }

    private static string GetRandomCity(Random random) => new[] { "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose" }[random.Next(10)];
    
    private static string GetRandomState(Random random) => new[] { "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI" }[random.Next(10)];
    
    private static string GetRandomLocation(Random random) => new[] { "Warehouse A", "Warehouse B", "Store 1", "Store 2", "Distribution Center", "Retail Location" }[random.Next(6)];
    
    private static string GetRandomCategory(Random random) => new[] { "Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Toys" }[random.Next(6)];

    private static TransactionType GetRandomTransactionType(Random random) => 
        (TransactionType)random.Next(1, 6);

    private static PaymentMethod GetRandomPaymentMethod(Random random) => 
        (PaymentMethod)random.Next(1, 6);

    private static TransactionStatus GetRandomTransactionStatus(Random random) => 
        (TransactionStatus)random.Next(1, 6);

    private static InventoryStatus GetRandomInventoryStatus(Random random) => 
        (InventoryStatus)random.Next(1, 6);

    private static NotificationType GetRandomNotificationType(Random random) => 
        (NotificationType)random.Next(1, 8);

    private static NotificationPriority GetRandomNotificationPriority(Random random) => 
        (NotificationPriority)random.Next(1, 6);

    private static NotificationChannel GetRandomNotificationChannel(Random random) => 
        (NotificationChannel)random.Next(1, 7);

    private static EventType GetRandomEventType(Random random) => 
        (EventType)random.Next(1, 16);
}
