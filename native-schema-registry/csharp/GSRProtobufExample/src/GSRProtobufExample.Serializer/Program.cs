using Example.V1;
using GSRProtobufExample.Serializer.Services;
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
        .CreateTopicIfNotExists("users", 1, 1)
        .CreateTopicIfNotExists("products", 1, 1)
        .CreateTopicIfNotExists("orders", 1, 1)
        .CreateTopicIfNotExists("events", 1, 1)
        .CreateTopicIfNotExists("companies", 1, 1)
        .AddProducer<UserProducer>(producer => producer
            .DefaultTopic("users")
            .AddMiddlewares(m => m
                .AddSerializer<GsrProtobufSerializer>()
            )
        )
        .AddProducer<ProductProducer>(producer => producer
            .DefaultTopic("products")
            .AddMiddlewares(m => m
                .AddSerializer<GsrProtobufSerializer>()
            )
        )
        .AddProducer<OrderProducer>(producer => producer
            .DefaultTopic("orders")
            .AddMiddlewares(m => m
                .AddSerializer<GsrProtobufSerializer>()
            )
        )
        .AddProducer<EventProducer>(producer => producer
            .DefaultTopic("events")
            .AddMiddlewares(m => m
                .AddSerializer<GsrProtobufSerializer>()
            )
        )
        .AddProducer<CompanyProducer>(producer => producer
            .DefaultTopic("companies")
            .AddMiddlewares(m => m
                .AddSerializer<GsrProtobufSerializer>()
            )
        )
    )
);

// Add application services
builder.Services.AddSingleton<MessagePublisher>();

var host = builder.Build();

// Start Kafka
var kafkaBus = host.Services.CreateKafkaBus();
await kafkaBus.StartAsync();

// Get message publisher and start publishing
var publisher = host.Services.GetRequiredService<MessagePublisher>();
var logger = host.Services.GetRequiredService<ILogger<Program>>();

logger.LogInformation("Starting GSR Protobuf Example Serializer...");

// Publish different types of messages
await publisher.PublishSampleMessagesAsync();

logger.LogInformation("All sample messages published. Press any key to exit...");
Console.ReadKey();

await kafkaBus.StopAsync();
