using GSRProtobufExample.Deserializer.Services;
using KafkaFlow;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

var builder = Host.CreateApplicationBuilder(args);

// Add logging
builder.Services.AddLogging(configure => configure.AddConsole());

// Add KafkaFlow (without custom log handler)
builder.Services.AddKafka(kafka => kafka
    .AddCluster(cluster => cluster
        .WithBrokers("localhost:9092")
        .AddConsumer(consumer => consumer
            .Topic("users")
            .WithGroupId("gsr-protobuf-example-users")
            .WithBufferSize(100)
            .WithWorkersCount(10)
            .AddMiddlewares(middlewares => middlewares
                .AddDeserializer<GsrProtobufDeserializer>()
                .AddTypedHandlers(h => h.AddHandler<UserHandler>())
            )
        )
        .AddConsumer(consumer => consumer
            .Topic("products")
            .WithGroupId("gsr-protobuf-example-products")
            .WithBufferSize(100)
            .WithWorkersCount(10)
            .AddMiddlewares(middlewares => middlewares
                .AddDeserializer<GsrProtobufDeserializer>()
                .AddTypedHandlers(h => h.AddHandler<ProductHandler>())
            )
        )
        .AddConsumer(consumer => consumer
            .Topic("orders")
            .WithGroupId("gsr-protobuf-example-orders")
            .WithBufferSize(100)
            .WithWorkersCount(10)
            .AddMiddlewares(middlewares => middlewares
                .AddDeserializer<GsrProtobufDeserializer>()
                .AddTypedHandlers(h => h.AddHandler<OrderHandler>())
            )
        )
        .AddConsumer(consumer => consumer
            .Topic("events")
            .WithGroupId("gsr-protobuf-example-events")
            .WithBufferSize(100)
            .WithWorkersCount(10)
            .AddMiddlewares(middlewares => middlewares
                .AddDeserializer<GsrProtobufDeserializer>()
                .AddTypedHandlers(h => h.AddHandler<EventHandler>())
            )
        )
        .AddConsumer(consumer => consumer
            .Topic("companies")
            .WithGroupId("gsr-protobuf-example-companies")
            .WithBufferSize(100)
            .WithWorkersCount(10)
            .AddMiddlewares(middlewares => middlewares
                .AddDeserializer<GsrProtobufDeserializer>()
                .AddTypedHandlers(h => h.AddHandler<CompanyHandler>())
            )
        )
    )
);

var host = builder.Build();

// Start Kafka
var kafkaBus = host.Services.CreateKafkaBus();
await kafkaBus.StartAsync();

var logger = host.Services.GetRequiredService<ILogger<Program>>();
logger.LogInformation("GSR Protobuf Example Deserializer started. Listening for messages...");
logger.LogInformation("Press Ctrl+C to exit");

// Handle shutdown
var tcs = new TaskCompletionSource<object>();
Console.CancelKeyPress += (_, _) => tcs.TrySetResult(new object());

await tcs.Task;

logger.LogInformation("Shutting down...");
await kafkaBus.StopAsync();
