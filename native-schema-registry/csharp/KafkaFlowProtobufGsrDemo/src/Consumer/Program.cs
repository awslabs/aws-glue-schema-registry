using KafkaFlow;
using KafkaFlowProtobufGsrDemo.Consumer;
using KafkaFlowProtobufGsrDemo.Consumer.MessageHandlers;
using KafkaFlowProtobufGsrDemo.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        services.AddKafka(kafka => kafka
            .UseLogHandler<ConsoleLogHandler>()
            .AddCluster(cluster => cluster
                .WithBrokers(new[] { "localhost:9092" })
                .AddConsumer(consumer => consumer
                    .Topic("customers")
                    .WithGroupId("customer-consumer-group")
                    .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                    .AddMiddlewares(middlewares => middlewares
                        .AddDeserializer<GsrProtobufDeserializer<Customer>>()
                        .AddTypedHandlers(h => h.AddHandler<CustomerHandler>())
                    )
                )
                .AddConsumer(consumer => consumer
                    .Topic("transactions")
                    .WithGroupId("transaction-consumer-group")
                    .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                    .AddMiddlewares(middlewares => middlewares
                        .AddDeserializer<GsrProtobufDeserializer<Transaction>>()
                        .AddTypedHandlers(h => h.AddHandler<TransactionHandler>())
                    )
                )
                .AddConsumer(consumer => consumer
                    .Topic("inventory")
                    .WithGroupId("inventory-consumer-group")
                    .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                    .AddMiddlewares(middlewares => middlewares
                        .AddDeserializer<GsrProtobufDeserializer<Inventory>>()
                        .AddTypedHandlers(h => h.AddHandler<InventoryHandler>())
                    )
                )
                .AddConsumer(consumer => consumer
                    .Topic("notifications")
                    .WithGroupId("notification-consumer-group")
                    .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                    .AddMiddlewares(middlewares => middlewares
                        .AddDeserializer<GsrProtobufDeserializer<Notification>>()
                        .AddTypedHandlers(h => h.AddHandler<NotificationHandler>())
                    )
                )
                .AddConsumer(consumer => consumer
                    .Topic("analytics")
                    .WithGroupId("analytics-consumer-group")
                    .WithAutoOffsetReset(AutoOffsetReset.Earliest)
                    .AddMiddlewares(middlewares => middlewares
                        .AddDeserializer<GsrProtobufDeserializer<Analytics>>()
                        .AddTypedHandlers(h => h.AddHandler<AnalyticsHandler>())
                    )
                )
            )
        );
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
