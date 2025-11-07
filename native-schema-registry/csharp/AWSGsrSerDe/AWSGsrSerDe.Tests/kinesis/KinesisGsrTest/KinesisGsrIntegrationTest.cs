using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Kinesis.ClientLibrary;
using AWSGsrSerDe.serializer;
using AWSGsrSerDe.deserializer;
using AWSGsrSerDe.serializer.json;
using AWSGsrSerDe.Tests.utils;
using AWSGsrSerDe.Tests.serializer.json;
using AWSGsrSerDe.Tests.Kinesis;
using Avro.Generic;
using NUnit.Framework;

namespace KinesisGsrTest
{
    [TestFixture]
    public class KinesisGsrIntegrationTest
    {
        private readonly string _localStackEndpoint = Environment.GetEnvironmentVariable("LOCALSTACK_ENDPOINT") ?? "http://localhost:4566";
        private string _streamName;
        private AmazonKinesisClient _kinesisClient;

        private static readonly string AVRO_CONFIG_PATH = "test-configs/avro-config.properties";
        private static readonly string JSON_CONFIG_PATH = "test-configs/json-config.properties";

        [SetUp]
        public async Task SetUp()
        {
            _streamName = $"gsr-test-{Guid.NewGuid().ToString("N")[..8]}";
            
            var kinesisConfig = new AmazonKinesisConfig
            {
                ServiceURL = _localStackEndpoint,
                UseHttp = true
            };
            
            _kinesisClient = new AmazonKinesisClient(kinesisConfig);
            await CreateStream();
        }

        [TearDown]
        public void TearDown()
        {
            _kinesisClient?.Dispose();
        }

        public async Task RunTest()
        {
            await SetUp();
            try
            {
                await TestKCLWithGSR_AVRO();
            }
            finally
            {
                TearDown();
            }
        }

        [Test]
        public async Task TestKCLWithGSR_AVRO()
        {
            TestContext.WriteLine("Testing KCL with GSR - AVRO");
            
            var serializer = new GlueSchemaRegistryKafkaSerializer(AVRO_CONFIG_PATH);
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(AVRO_CONFIG_PATH);
            
            var avroRecord = RecordGenerator.GetTestAvroRecord();
            var processor = new GsrRecordProcessor(deserializer);
            
            // Start KCL consumer
            var kclTask = StartKCLConsumer(processor);
            await Task.Delay(2000);
            
            // Produce record
            await ProduceRecord(avroRecord, serializer, "avro-test");
            await Task.Delay(3000);
            
            Assert.IsTrue(processor.CreationSuccess);
            Assert.IsTrue(processor.ConsumptionSuccess);
            Assert.AreEqual(1, processor.ConsumedRecords.Count);
        }

        [Test]
        public async Task TestKCLWithGSR_JSON()
        {
            TestContext.WriteLine("Testing KCL with GSR - JSON");
            
            var serializer = new GlueSchemaRegistryKafkaSerializer(JSON_CONFIG_PATH);
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(JSON_CONFIG_PATH);
            
            var jsonRecord = RecordGenerator.GetSampleJsonTestData();
            var processor = new GsrRecordProcessor(deserializer);
            
            // Start KCL consumer
            var kclTask = StartKCLConsumer(processor);
            await Task.Delay(2000);
            
            // Produce record
            await ProduceRecord(jsonRecord, serializer, "json-test");
            await Task.Delay(3000);
            
            Assert.IsTrue(processor.CreationSuccess);
            Assert.IsTrue(processor.ConsumptionSuccess);
            Assert.AreEqual(1, processor.ConsumedRecords.Count);
        }

        private async Task CreateStream()
        {
            try
            {
                await _kinesisClient.CreateStreamAsync(new CreateStreamRequest
                {
                    StreamName = _streamName,
                    ShardCount = 1
                });
                
                // Wait for active
                while (true)
                {
                    var response = await _kinesisClient.DescribeStreamAsync(new DescribeStreamRequest { StreamName = _streamName });
                    if (response.StreamDescription.StreamStatus == StreamStatus.ACTIVE)
                        break;
                    await Task.Delay(1000);
                }
            }
            catch (ResourceInUseException) { }
        }

        private async Task ProduceRecord<T>(T record, GlueSchemaRegistryKafkaSerializer serializer, string topic)
        {
            var data = serializer.Serialize(record, topic);
            await _kinesisClient.PutRecordAsync(new PutRecordRequest
            {
                StreamName = _streamName,
                PartitionKey = Guid.NewGuid().ToString(),
                Data = new MemoryStream(data)
            });
        }

        private async Task StartKCLConsumer(GsrRecordProcessor processor)
        {
            // Simplified KCL simulation for testing
            TestContext.WriteLine("Starting KCL consumer simulation...");
            
            await Task.Run(async () =>
            {
                try
                {
                    // Initialize processor
                    var mockInitInput = new MockInitializationInput("shard-000000000000");
                    processor.Initialize(mockInitInput);
                    
                    // Simulate polling
                    for (int i = 0; i < 10; i++)
                    {
                        await Task.Delay(500);
                        
                        // In real KCL, this would be handled by the library
                        var describeRequest = new DescribeStreamRequest { StreamName = _streamName };
                        var response = await _kinesisClient.DescribeStreamAsync(describeRequest);
                        
                        if (response.StreamDescription.Shards.Count > 0)
                        {
                            var shardId = response.StreamDescription.Shards[0].ShardId;
                            var records = await ConsumeRecordsFromShard(shardId);
                            
                            if (records.Count > 0)
                            {
                                var mockProcessInput = new MockProcessRecordsInput(records);
                                processor.ProcessRecords(mockProcessInput);
                                break;
                            }
                        }
                    }
                    
                    // Shutdown
                    var mockShutdownInput = new MockShutdownRequestedInput();
                    processor.ShutdownRequested(mockShutdownInput);
                }
                catch (Exception ex)
                {
                    TestContext.WriteLine($"KCL simulation error: {ex.Message}");
                }
            });
        }

        private async Task<IList<Amazon.Kinesis.ClientLibrary.Record>> ConsumeRecordsFromShard(string shardId)
        {
            var records = new List<Amazon.Kinesis.ClientLibrary.Record>();
            
            try
            {
                var shardIteratorRequest = new GetShardIteratorRequest
                {
                    StreamName = _streamName,
                    ShardId = shardId,
                    ShardIteratorType = ShardIteratorType.TRIM_HORIZON
                };
                
                var shardIteratorResponse = await _kinesisClient.GetShardIteratorAsync(shardIteratorRequest);
                var shardIterator = shardIteratorResponse.ShardIterator;
                
                var getRecordsRequest = new GetRecordsRequest { ShardIterator = shardIterator };
                var getRecordsResponse = await _kinesisClient.GetRecordsAsync(getRecordsRequest);
                
                foreach (var record in getRecordsResponse.Records)
                {
                    // Convert Kinesis record to KCL record format
                    var kclRecord = new MockKCLRecord(
                        record.Data.ToArray(),
                        record.PartitionKey,
                        record.SequenceNumber
                    );
                    records.Add(kclRecord);
                }
            }
            catch (Exception ex)
            {
                TestContext.WriteLine($"Error consuming records: {ex.Message}");
            }
            
            return records;
        }
    }

    public class GsrRecordProcessor : IShardRecordProcessor
    {
        private readonly GlueSchemaRegistryKafkaDeserializer _deserializer;
        public bool CreationSuccess { get; set; }
        public bool ConsumptionSuccess { get; set; }
        public List<object> ConsumedRecords { get; } = new List<object>();

        public GsrRecordProcessor(GlueSchemaRegistryKafkaDeserializer deserializer)
        {
            _deserializer = deserializer;
        }

        public void Initialize(InitializationInput input)
        {
            TestContext.WriteLine($"Initializing shard: {input.ShardId}");
            CreationSuccess = true;
        }

        public void ProcessRecords(ProcessRecordsInput input)
        {
            TestContext.WriteLine($"Processing {input.Records.Count} records");
            
            foreach (var record in input.Records)
            {
                try
                {
                    var data = record.Data;
                    var deserializedRecord = _deserializer.Deserialize("test-topic", data);
                    ConsumedRecords.Add(deserializedRecord);
                }
                catch (Exception ex)
                {
                    TestContext.WriteLine($"Error: {ex.Message}");
                }
            }
            
            ConsumptionSuccess = ConsumedRecords.Count > 0;
        }

        public void LeaseLost(LeaseLossInput input) { }
        public void ShardEnded(ShardEndedInput input) { }
        public void ShutdownRequested(ShutdownRequestedInput input) { }
    }
}
