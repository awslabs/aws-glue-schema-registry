using KafkaFlow;
using KafkaFlow.Producers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using ProtobufGSRKafkaDemo.GSR;
using ProtobufGSRKafkaDemo.Messages;
using Google.Protobuf.WellKnownTypes;

namespace ProtobufGSRKafkaDemo.Producer;

class Program
{
    private static readonly string CONFIG_PATH = "../config/gsr.properties";
    private static volatile bool _shutdown = false;
    private static readonly Thread[] _threads = new Thread[5];
    private static readonly object _lock = new object();

    static async Task Main(string[] args)
    {
        Console.WriteLine("=== Multi-threaded Protobuf GSR Kafka Producer ===");
        Console.WriteLine("Starting 5 producer threads...\n");

        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            _shutdown = true;
            Console.WriteLine("\nShutdown requested...");
        };

        var host = CreateHost();
        await host.StartAsync();

        // Start 5 producer threads
        _threads[0] = new Thread(() => ProduceUsers(host)) { Name = "UserProducer" };
        _threads[1] = new Thread(() => ProduceProducts(host)) { Name = "ProductProducer" };
        _threads[2] = new Thread(() => ProduceOrders(host)) { Name = "OrderProducer" };
        _threads[3] = new Thread(() => ProducePayments(host)) { Name = "PaymentProducer" };
        _threads[4] = new Thread(() => ProduceEvents(host)) { Name = "EventProducer" };

        foreach (var thread in _threads)
        {
            thread.Start();
        }


        // Cleanup
        Console.WriteLine("Stopping threads...");
        foreach (var thread in _threads)
        {
            thread.Join(5000);
        }

        await host.StopAsync();
        Console.WriteLine("Producer stopped.");
    }

    static IHost CreateHost()

    {
        string brokerString = Environment.GetEnvironmentVariable("KAFKA_BROKER") ;
        return Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddKafka(kafka => kafka
                    .AddCluster(cluster => cluster
                        .WithBrokers(new[] { brokerString })
                        .CreateTopicIfNotExists("users", 1, 1)
                        .CreateTopicIfNotExists("products", 1, 1)
                        .CreateTopicIfNotExists("orders", 1, 1)
                        .CreateTopicIfNotExists("payments", 1, 1)
                        .CreateTopicIfNotExists("events", 1, 1)
                        .AddProducer("user-producer", producer => producer
                            .DefaultTopic("users")
                            .AddMiddlewares(middlewares => middlewares
                                .Add<ExceptionMiddleware>()
                                .AddSerializer(resolver => new GsrProtobufSerializer<User>(CONFIG_PATH))
                            )
                        )
                        .AddProducer("product-producer", producer => producer
                            .DefaultTopic("products")
                            .AddMiddlewares(middlewares => middlewares
                                .Add<ExceptionMiddleware>()
                                .AddSerializer(resolver => new GsrProtobufSerializer<Product>(CONFIG_PATH))
                            )
                        )
                        .AddProducer("order-producer", producer => producer
                            .DefaultTopic("orders")
                            .AddMiddlewares(middlewares => middlewares
                                .Add<ExceptionMiddleware>()
                                .AddSerializer(resolver => new GsrProtobufSerializer<Order>(CONFIG_PATH))
                            )
                        )
                        .AddProducer("payment-producer", producer => producer
                            .DefaultTopic("payments")
                            .AddMiddlewares(middlewares => middlewares
                                .Add<ExceptionMiddleware>()
                                .AddSerializer(resolver => new GsrProtobufSerializer<Payment>(CONFIG_PATH))
                            )
                        )
                        .AddProducer("event-producer", producer => producer
                            .DefaultTopic("events")
                            .AddMiddlewares(middlewares => middlewares
                                .Add<ExceptionMiddleware>()
                                .AddSerializer(resolver => new GsrProtobufSerializer<Event>(CONFIG_PATH))
                            )
                        )
                    )
                );
            })
            .Build();
    }

    static void ProduceUsers(IHost host)
    {
        var producer = host.Services.GetRequiredService<IProducerAccessor>().GetProducer("user-producer");
        
        for (int counter = 1; counter <= 10 && !_shutdown; counter++)
        {
            try
            {
                var user = new User
                {
                    Id = $"user-{counter}",
                    Name = $"User {counter}",
                    Email = $"user{counter}@example.com",
                    Status = UserStatus.Active
                };

                producer.Produce(user.Id, user);
                
                lock (_lock)
                {
                    Console.WriteLine($"[UserProducer] Sent: {user.Name} ({user})");
                }
                
                Thread.Sleep(1000);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[UserProducer] Error: {ex.Message}");
            }
        }
        
        lock (_lock)
        {
            Console.WriteLine($"[UserProducer] Completed");
        }
    }

    static void ProduceProducts(IHost host)
    {
        var producer = host.Services.GetRequiredService<IProducerAccessor>().GetProducer("product-producer");
        
        for (int counter = 1; counter <= 10 && !_shutdown; counter++)
        {
            try
            {
                var product = new Product
                {
                    Sku = $"SKU-{counter:D4}",
                    Name = $"Product {counter}",
                    Price = 19.99 + counter,
                    Category = ProductCategory.Electronics,
                    InStock = counter % 2 == 0
                };

                producer.Produce(product.Sku, product);
                
                lock (_lock)
                {
                    Console.WriteLine($"[ProductProducer] Sent: {product.Name} (${product})");
                }
                
                Thread.Sleep(1200);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ProductProducer] Error: {ex.Message}");
            }
        }
        
        lock (_lock)
        {
            Console.WriteLine($"[ProductProducer] Completed");
        }
    }

    static void ProduceOrders(IHost host)
    {
        var producer = host.Services.GetRequiredService<IProducerAccessor>().GetProducer("order-producer");
        
        for (int counter = 1; counter <= 10 && !_shutdown; counter++)
        {
            try
            {
                var order = new Order
                {
                    OrderId = $"order-{counter}",
                    CustomerId = $"customer-{counter}",
                    Header = new OrderHeader
                    {
                        CreatedTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        TotalAmount = 99.99,
                        Currency = "USD",
                        PaymentMethod = PaymentMethod.CreditCard
                    },
                    ShippingAddress = new ShippingAddress
                    {
                        Street = "123 Main St",
                        City = "Seattle",
                        State = "WA",
                        ZipCode = "98101",
                        Country = "USA"
                    },
                    Status = OrderStatus.Pending
                };

                order.Items.Add(new OrderItem
                {
                    ProductId = $"prod-{counter}",
                    Name = $"Item {counter}",
                    Quantity = 2,
                    UnitPrice = 49.99,
                    TotalPrice = 99.98
                });

                order.Metadata.Add("source", "web");
                order.Discounts.Add("promo", 5.0);

                producer.Produce(order.OrderId, order);
                
                lock (_lock)
                {
                    Console.WriteLine($"[OrderProducer] Sent: Order {order.OrderId} (${order.Header})");
                }
                
                Thread.Sleep(1500);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[OrderProducer] Error: {ex.Message}");
            }
        }
        
        lock (_lock)
        {
            Console.WriteLine($"[OrderProducer] Completed");
        }
    }

    static void ProducePayments(IHost host)
    {
        var producer = host.Services.GetRequiredService<IProducerAccessor>().GetProducer("payment-producer");
        
        for (int counter = 1; counter <= 10 && !_shutdown; counter++)
        {
            try
            {
                var payment = new Payment
                {
                    PaymentId = $"pay-{counter}",
                    OrderId = $"order-{counter}",
                    Amount = 99.99,
                    Currency = "USD",
                    Details = new PaymentDetails
                    {
                        ProcessedTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                        Processor = "Stripe",
                        TransactionId = $"txn-{counter}",
                        AuthorizationCode = $"auth-{counter}"
                    },
                    BillingAddress = new BillingAddress
                    {
                        Name = $"Customer {counter}",
                        Street = "456 Oak Ave",
                        City = "Portland",
                        State = "OR",
                        ZipCode = "97201",
                        Country = "USA"
                    },
                    Status = PaymentStatus.Completed,
                    CreditCard = new CreditCard
                    {
                        LastFourDigits = "1234",
                        CardType = "Visa",
                        ExpiryMonth = 12,
                        ExpiryYear = 2025
                    }
                };

                payment.Fees.Add(new TransactionFee
                {
                    Type = "processing",
                    Amount = 2.99,
                    Description = "Payment processing fee"
                });

                producer.Produce(payment.PaymentId, payment);
                
                lock (_lock)
                {
                    Console.WriteLine($"[PaymentProducer] Sent: Payment {payment.PaymentId} (${payment})");
                }
                
                Thread.Sleep(1800);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[PaymentProducer] Error: {ex.Message}");
            }
        }
        
        lock (_lock)
        {
            Console.WriteLine($"[PaymentProducer] Completed");
        }
    }

    static void ProduceEvents(IHost host)
    {
        var producer = host.Services.GetRequiredService<IProducerAccessor>().GetProducer("event-producer");
        
        for (int counter = 1; counter <= 10 && !_shutdown; counter++)
        {
            try
            {
                var eventMsg = new Event
                {
                    EventId = $"event-{counter}",
                    UserId = $"user-{counter}",
                    SessionId = $"session-{counter}",
                    Metadata = new EventMetadata
                    {
                        Source = "web-app",
                        Version = "1.0",
                        CorrelationId = Guid.NewGuid().ToString(),
                        SequenceNumber = counter
                    },
                    UserContext = new UserContext
                    {
                        IpAddress = "192.168.1.100",
                        UserAgent = "Mozilla/5.0",
                        Referrer = "https://google.com",
                        Location = new Location
                        {
                            Country = "USA",
                            Region = "WA",
                            City = "Seattle"
                        }
                    },
                    DeviceInfo = new DeviceInfo
                    {
                        DeviceId = $"device-{counter}",
                        Platform = "web",
                        OsVersion = "Windows 11",
                        AppVersion = "1.0.0",
                        IsMobile = false
                    },
                    Timestamp = Timestamp.FromDateTime(DateTime.UtcNow),
                    DurationMs = 1500.0,
                    Type = EventType.PageView
                };

                eventMsg.Properties.Add("page", "/home");
                eventMsg.Properties.Add("action", "view");
                eventMsg.Tags.Add("environment", "prod");

                producer.Produce(eventMsg.EventId, eventMsg);
                
                lock (_lock)
                {
                    Console.WriteLine($"[EventProducer] Sent: Event {eventMsg.EventId} ({eventMsg})");
                }
                
                Thread.Sleep(800);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[EventProducer] Error: {ex.Message}");
            }
        }
        
        lock (_lock)
        {
            Console.WriteLine($"[EventProducer] Completed");
        }
    }
}