using KafkaFlow;
using KafkaFlow.Producers;
using KafkaFlowProtobufGsrDemo.Messages;
using KafkaFlowProtobufGsrDemo.Producer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddKafka(kafka => kafka
            .UseLogHandler<ConsoleLogHandler>()
            .AddCluster(cluster => cluster
                .WithBrokers(new[] { "localhost:9092" })
                .CreateTopicIfNotExists("customers", 1, 1)
                .CreateTopicIfNotExists("transactions", 1, 1)
                .CreateTopicIfNotExists("inventory", 1, 1)
                .CreateTopicIfNotExists("notifications", 1, 1)
                .CreateTopicIfNotExists("analytics", 1, 1)
                .AddProducer("customer-producer", producer => producer
                    .DefaultTopic("customers")
                    .AddMiddlewares(middlewares => middlewares
                        .AddSerializer<GsrProtobufSerializer<Customer>>()
                    )
                )
                .AddProducer("transaction-producer", producer => producer
                    .DefaultTopic("transactions")
                    .AddMiddlewares(middlewares => middlewares
                        .AddSerializer<GsrProtobufSerializer<Transaction>>()
                    )
                )
                .AddProducer("inventory-producer", producer => producer
                    .DefaultTopic("inventory")
                    .AddMiddlewares(middlewares => middlewares
                        .AddSerializer<GsrProtobufSerializer<Inventory>>()
                    )
                )
                .AddProducer("notification-producer", producer => producer
                    .DefaultTopic("notifications")
                    .AddMiddlewares(middlewares => middlewares
                        .AddSerializer<GsrProtobufSerializer<Notification>>()
                    )
                )
                .AddProducer("analytics-producer", producer => producer
                    .DefaultTopic("analytics")
                    .AddMiddlewares(middlewares => middlewares
                        .AddSerializer<GsrProtobufSerializer<Analytics>>()
                    )
                )
            )
        );
        
        services.AddHostedService<MessageProducerService>();
    })
    .Build();

await host.RunAsync();

public class ConsoleLogHandler : ILogHandler
{
    public void Error(string message, Exception ex, object data)
    {
        Console.WriteLine($"ERROR: {message} - {ex?.Message}");
    }

    public void Info(string message, object data)
    {
        Console.WriteLine($"INFO: {message}");
    }

    public void Warning(string message, object data)
    {
        Console.WriteLine($"WARNING: {message}");
    }

    public void Verbose(string message, object data)
    {
        Console.WriteLine($"VERBOSE: {message}");
    }
}
