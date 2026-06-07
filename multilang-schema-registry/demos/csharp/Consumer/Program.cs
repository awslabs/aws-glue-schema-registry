using KafkaFlow;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ProtobufGSRKafkaDemo.Messages;
using AWSGsrSerDe.KafkaFlow;


namespace ProtobufGSRKafkaDemo.Consumer;

class Program
{
    private static readonly string CONFIG_PATH = "../config/gsr.properties";
    private static volatile bool _shutdown = false;
    private static readonly Thread[] _threads = new Thread[5];
    private static readonly object _lock = new object();

    static async Task Main(string[] args)
    {
        Console.WriteLine("=== Multi-threaded Protobuf GSR Kafka Consumer ===");
        Console.WriteLine("Starting 5 consumer threads...\n");
        Console.WriteLine($"GSR Config: {CONFIG_PATH}");
        Console.WriteLine("Initializing KafkaFlow consumers...");

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            _shutdown = true;
            Console.WriteLine("\nShutdown requested...");
        };

        try
        {
            var host = CreateHost();
            Console.WriteLine("Starting Kafka consumers...");
            await host.StartAsync();
            
            // Wait a moment for KafkaFlow to initialize
            Console.WriteLine("Waiting for KafkaFlow to initialize...");
            await Task.Delay(5000);
            Console.WriteLine("✅ KafkaFlow initialization complete!");
            Console.WriteLine("✅ All consumers started successfully!");
            Console.WriteLine("Waiting for messages...");

        // Start 5 consumer threads
        _threads[0] = new Thread(() => ConsumeUsers(host)) { Name = "UserConsumer" };
        _threads[1] = new Thread(() => ConsumeProducts(host)) { Name = "ProductConsumer" };
        _threads[2] = new Thread(() => ConsumeOrders(host)) { Name = "OrderConsumer" };
        _threads[3] = new Thread(() => ConsumePayments(host)) { Name = "PaymentConsumer" };
        _threads[4] = new Thread(() => ConsumeEvents(host)) { Name = "EventConsumer" };

        foreach (var thread in _threads)
        {
            thread.Start();
        }

        // Wait for shutdown

        // Cleanup
        Console.WriteLine("Stopping threads...");
        foreach (var thread in _threads)
        {
            thread.Join(1000);
        }

            await host.StopAsync();
            Console.WriteLine("Consumer stopped.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Consumer startup failed: {ex.Message}");
            Console.WriteLine($"Stack trace: {ex.StackTrace}");
        }
    }

    static IHost CreateHost()
    {
        string brokerString = Environment.GetEnvironmentVariable("KAFKA_BROKER") ;
        return Host.CreateDefaultBuilder()
            .ConfigureLogging(logging =>
            {
                logging.SetMinimumLevel(LogLevel.Debug);
                logging.AddConsole();
            })
            .ConfigureServices(services =>
            {
                services.AddKafkaFlowHostedService(kafka => kafka
                    .AddCluster(cluster => cluster
                        .WithBrokers(new[] { brokerString })
                        .AddConsumer(consumer => consumer
                            .Topic("users")
                            .WithGroupId("user-consumer-group")
                            .WithBufferSize(100)
                            .WithWorkersCount(15)
                            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                            .AddMiddlewares(middlewares => middlewares
                                .Add<ExceptionMiddleware>()
                                .AddDeserializer(resolver => new GlueSchemaRegistryKafkaFlowProtobufDeserializer<User>(CONFIG_PATH))
                                .AddTypedHandlers(h => h.AddHandler<UserHandler>())
                            )
                        )
                        .AddConsumer(consumer => consumer
                            .Topic("products")
                            .WithGroupId("product-consumer-group")
                            .WithBufferSize(100)
                            .WithWorkersCount(15)
                            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                            .AddMiddlewares(middlewares => middlewares
                                .Add<ExceptionMiddleware>() 
                                .AddDeserializer(resolver => new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Product>(CONFIG_PATH))
                                .AddTypedHandlers(h => h.AddHandler<ProductHandler>())
                            )
                        )
                        .AddConsumer(consumer => consumer
                            .Topic("orders")
                            .WithGroupId("order-consumer-group")
                            .WithBufferSize(100)
                            .WithWorkersCount(15)
                            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                            .AddMiddlewares(middlewares => middlewares
                                .Add<ExceptionMiddleware>()
                                .AddDeserializer(resolver => new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Order>(CONFIG_PATH))
                                .AddTypedHandlers(h => h.AddHandler<OrderHandler>())
                            )
                        )
                        .AddConsumer(consumer => consumer
                            .Topic("payments")
                            .WithGroupId("payment-consumer-group")
                            .WithBufferSize(100)
                            .WithWorkersCount(15)
                            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                            .AddMiddlewares(middlewares => middlewares
                                .Add<ExceptionMiddleware>()
                                .AddDeserializer(resolver => new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Payment>(CONFIG_PATH))
                                .AddTypedHandlers(h => h.AddHandler<PaymentHandler>())
                            )
                        )
                        .AddConsumer(consumer => consumer
                            .Topic("events")
                            .WithGroupId("event-consumer-group")
                            .WithBufferSize(100)
                            .WithWorkersCount(15)
                            .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                            .AddMiddlewares(middlewares => middlewares 
                                .Add<ExceptionMiddleware>()
                                .AddDeserializer(resolver => new GlueSchemaRegistryKafkaFlowProtobufDeserializer<Event>(CONFIG_PATH))
                                .AddTypedHandlers(h => h.AddHandler<EventHandler>())
                            )
                        )
                    )
                );
            })
            .Build();
    }

    // Dummy thread methods - KafkaFlow handles the actual consumption
    static void ConsumeUsers(IHost host)
    {
        while (!_shutdown)
        {
            Thread.Sleep(1000);
        }
    }

    static void ConsumeProducts(IHost host)
    {
        while (!_shutdown)
        {
            Thread.Sleep(1000);
        }
    }

    static void ConsumeOrders(IHost host)
    {
        while (!_shutdown)
        {
            Thread.Sleep(1000);
        }
    }

    static void ConsumePayments(IHost host)
    {
        while (!_shutdown)
        {
            Thread.Sleep(1000);
        }
    }

    static void ConsumeEvents(IHost host)
    {
        while (!_shutdown)
        {
            Thread.Sleep(1000);
        }
    }
}

public class UserHandler : IMessageHandler<User>
{
    public Task Handle(IMessageContext context, User message)
    {
        try
        {
            var workerId = Thread.CurrentThread.ManagedThreadId;
            Console.WriteLine($"[UserConsumer-W{workerId}] SUCCESS: Received {message.Name} ({message.Id}) - Status: {message.Status}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[UserConsumer] ERROR handling message: {ex.Message}");
        }
        return Task.CompletedTask;
    }
}

public class ProductHandler : IMessageHandler<Product>
{
    public Task Handle(IMessageContext context, Product message)
    {
        try
        {
            var workerId = Thread.CurrentThread.ManagedThreadId;
            Console.WriteLine($"[ProductConsumer-W{workerId}] SUCCESS: Received {message.Name} (${message.Price}) - Category: {message.Category}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ProductConsumer] ERROR handling message: {ex.Message}");
        }
        return Task.CompletedTask;
    }
}

public class OrderHandler : IMessageHandler<Order>
{
    public Task Handle(IMessageContext context, Order message)
    {
        try
        {
            var workerId = Thread.CurrentThread.ManagedThreadId;
            Console.WriteLine($"[OrderConsumer-W{workerId}] SUCCESS: Received Order {message.OrderId} - Total: ${message.Header.TotalAmount} - Items: {message.Items.Count}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[OrderConsumer] ERROR handling message: {ex.Message}");
        }
        return Task.CompletedTask;
    }
}

public class PaymentHandler : IMessageHandler<Payment>
{
    public Task Handle(IMessageContext context, Payment message)
    {
        try
        {
            var workerId = Thread.CurrentThread.ManagedThreadId;
            var paymentMethod = message.PaymentMethodCase switch
            {
                Payment.PaymentMethodOneofCase.CreditCard => "Credit Card",
                Payment.PaymentMethodOneofCase.BankAccount => "Bank Account",
                Payment.PaymentMethodOneofCase.DigitalWallet => "Digital Wallet",
                _ => "Unknown"
            };
            
            Console.WriteLine($"[PaymentConsumer-W{workerId}] SUCCESS: Received Payment {message.PaymentId} - ${message.Amount} via {paymentMethod}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[PaymentConsumer] ERROR handling message: {ex.Message}");
        }
        return Task.CompletedTask;
    }
}

public class EventHandler : IMessageHandler<Event>
{
    public Task Handle(IMessageContext context, Event message)
    {
        try
        {
            var workerId = Thread.CurrentThread.ManagedThreadId;
            Console.WriteLine($"[EventConsumer-W{workerId}] SUCCESS: Received Event {message.EventId} - Type: {message.Type} - User: {message.UserId}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[EventConsumer] ERROR handling message: {ex.Message}");
        }
        return Task.CompletedTask;
    }
}
