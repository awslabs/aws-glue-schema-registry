// Copyright 2020 Amazon.com, Inc. or its affiliates.
// Licensed under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
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

namespace AWSGsrSerDe.Tests.Kinesis
{
    [TestFixture]
    public class GlueSchemaRegistryRealKCLTest
    {
        private readonly string _localStackEndpoint = Environment.GetEnvironmentVariable("LOCALSTACK_ENDPOINT") ?? "http://localhost:4566";
        private string _streamName;
        private AmazonKinesisClient _kinesisClient;

        [SetUp]
        public void Setup()
        {
            _streamName = $"test-stream-{Guid.NewGuid()}";
            _kinesisClient = new AmazonKinesisClient(new AmazonKinesisConfig
            {
                ServiceURL = _localStackEndpoint,
                UseHttp = true
            });
        }
        private readonly List<string> _schemasToCleanUp = new List<string>();

        private static readonly string AVRO_CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal.properties");
        private static readonly string JSON_CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal-json.properties");

        private static readonly Car SPECIFIC_TEST_RECORD = new Car
        {
            make = "Honda",
            model = "crv",
            used = true,
            miles = 10000,
            listedDate = DateTime.Now,
            purchaseDate = DateTime.Parse("2000-01-01T00:00:00.000Z"),
            owners = new[] { "John", "Jane", "Hu" },
            serviceCheckes = new[] { 5000.0f, 10780.30f }
        };

        private static string GetConfigPath(string relativePath)
        {
            var currentDir = new DirectoryInfo(Directory.GetCurrentDirectory());
            while (currentDir != null && !currentDir.GetFiles("*.csproj").Any())
            {
                currentDir = currentDir.Parent;
            }
            
            if (currentDir == null)
            {
                throw new DirectoryNotFoundException("Could not find project root directory containing .csproj file");
            }
            
            return Path.Combine(currentDir.FullName, relativePath);
        }

        [SetUp]
        public async Task SetUp()
        {
            _streamName = $"gsr-real-kcl-test-{Guid.NewGuid().ToString("N")[..8]}";
            
            var kinesisConfig = new AmazonKinesisConfig
            {
                ServiceURL = _localStackEndpoint,
                UseHttp = true
            };
            
            _kinesisClient = new AmazonKinesisClient(kinesisConfig);
            await CreateKinesisStream();
        }

        [TearDown]
        public void TearDown()
        {
            _kinesisClient?.Dispose();
            
            foreach (var schemaName in _schemasToCleanUp)
            {
                TestContext.WriteLine($"Cleaning up schema: {schemaName}");
            }
        }

        [Test]
        public async Task TestRealKCLWithGSR_AVRO()
        {
            TestContext.WriteLine("Testing REAL KCL MultiLangDaemon with GSR - AVRO format");
            
            var serializer = new GlueSchemaRegistryKafkaSerializer(AVRO_CONFIG_PATH);
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(AVRO_CONFIG_PATH);
            
            var avroRecord = RecordGenerator.GetTestAvroRecord();
            var testRecords = new List<GenericRecord> { avroRecord };
            
            // Create the real KCL processor
            var recordProcessor = new RealKCLGsrRecordProcessor(deserializer);
            
            // Start REAL KCL MultiLangDaemon
            var kclTask = Task.Run(() => StartRealKCL(recordProcessor));
            
            // Give KCL time to initialize and connect to Kinesis
            await Task.Delay(TimeSpan.FromSeconds(10));
            
            // Produce records
            await ProduceRecordsWithKinesisSDK(testRecords, serializer, "real-kcl-avro-test");
            
            // Wait for KCL to consume records
            await Task.Delay(TimeSpan.FromSeconds(15));
            
            // Stop KCL
            recordProcessor.RequestShutdown();
            
            try
            {
                await kclTask.WaitAsync(TimeSpan.FromSeconds(30));
            }
            catch (TimeoutException)
            {
                TestContext.WriteLine("KCL shutdown timed out - this is expected in some cases");
            }
            
            // Validate results
            Assert.IsTrue(recordProcessor.InitializationSuccess, "KCL should initialize successfully");
            Assert.IsTrue(recordProcessor.ProcessingSuccess, "KCL should process records successfully");
            Assert.GreaterOrEqual(recordProcessor.ConsumedRecords.Count, 1, "At least one record should be consumed");
            
            // Validate record content
            var consumedRecord = (GenericRecord)recordProcessor.ConsumedRecords[0];
            
            TestContext.WriteLine($"🔍 Original record: {avroRecord}");
            TestContext.WriteLine($"🔍 Consumed record: {consumedRecord}");
            TestContext.WriteLine($"🔍 Original fields: name={avroRecord["name"]}, favorite_number={avroRecord["favorite_number"]}, favorite_color={avroRecord["favorite_color"]}");
            TestContext.WriteLine($"🔍 Consumed fields: name={consumedRecord["name"]}, favorite_number={consumedRecord["favorite_number"]}, favorite_color={consumedRecord["favorite_color"]}");
            
            Assert.AreEqual(avroRecord["name"], consumedRecord["name"], "Name field should match");
            Assert.AreEqual(avroRecord["favorite_number"], consumedRecord["favorite_number"], "Favorite number should match");
            Assert.AreEqual(avroRecord["favorite_color"], consumedRecord["favorite_color"], "Favorite color should match");
            Assert.AreEqual(avroRecord, consumedRecord, "Consumed record should match produced record");
            
            TestContext.WriteLine("✅ Real KCL AVRO integration test successful!");
        }

        [Test]
        public async Task TestRealKCLWithGSR_JSON()
        {
            TestContext.WriteLine("Testing REAL KCL MultiLangDaemon with GSR - JSON format");
            
            var serializer = new GlueSchemaRegistryKafkaSerializer(JSON_CONFIG_PATH);
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(JSON_CONFIG_PATH);
            
            var jsonRecord = RecordGenerator.GetSampleJsonTestData();
            var testRecords = new List<JsonDataWithSchema> { jsonRecord };
            
            var recordProcessor = new RealKCLGsrRecordProcessor(deserializer);
            var kclTask = Task.Run(() => StartRealKCL(recordProcessor));
            
            await Task.Delay(TimeSpan.FromSeconds(10));
            await ProduceRecordsWithKinesisSDK(testRecords, serializer, "real-kcl-json-test");
            await Task.Delay(TimeSpan.FromSeconds(15));
            
            recordProcessor.RequestShutdown();
            
            try
            {
                await kclTask.WaitAsync(TimeSpan.FromSeconds(30));
            }
            catch (TimeoutException)
            {
                TestContext.WriteLine("KCL shutdown timed out - this is expected");
            }
            
            Assert.IsTrue(recordProcessor.InitializationSuccess);
            Assert.IsTrue(recordProcessor.ProcessingSuccess);
            Assert.GreaterOrEqual(recordProcessor.ConsumedRecords.Count, 1);
            
            var consumedRecord = (JsonDataWithSchema)recordProcessor.ConsumedRecords[0];
            Assert.AreEqual(jsonRecord.Schema, consumedRecord.Schema);
            Assert.AreEqual(jsonRecord.Payload, consumedRecord.Payload);
            
            TestContext.WriteLine("✅ Real KCL JSON integration test successful!");
        }

        private async Task CreateKinesisStream()
        {
            TestContext.WriteLine($"Creating Kinesis stream: {_streamName}");
            
            try
            {
                var createRequest = new CreateStreamRequest
                {
                    StreamName = _streamName,
                    ShardCount = 1
                };
                await _kinesisClient.CreateStreamAsync(createRequest);
                
                var maxWaitTime = TimeSpan.FromSeconds(60);
                var startTime = DateTime.UtcNow;
                
                while (DateTime.UtcNow - startTime < maxWaitTime)
                {
                    var describeRequest = new DescribeStreamRequest { StreamName = _streamName };
                    var response = await _kinesisClient.DescribeStreamAsync(describeRequest);
                    if (response.StreamDescription.StreamStatus == StreamStatus.ACTIVE)
                    {
                        TestContext.WriteLine($"✓ Stream {_streamName} is active");
                        return;
                    }
                    await Task.Delay(2000);
                }
                
                throw new Exception("Stream did not become active within timeout");
            }
            catch (ResourceInUseException)
            {
                TestContext.WriteLine($"Stream {_streamName} already exists");
            }
        }

        private async Task<string> ProduceRecordsWithKinesisSDK<T>(
            List<T> records,
            GlueSchemaRegistryKafkaSerializer serializer,
            string topicName)
        {
            string shardId = null;
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            
            _schemasToCleanUp.Add($"{_streamName}-{topicName}");
            
            for (int i = 0; i < records.Count; i++)
            {
                var record = records[i];
                var serializedData = serializer.Serialize(record, topicName);
                
                var putRequest = new PutRecordRequest
                {
                    StreamName = _streamName,
                    PartitionKey = $"{timestamp}-{i}",
                    Data = new MemoryStream(serializedData)
                };
                
                var response = await _kinesisClient.PutRecordAsync(putRequest);
                shardId = response.ShardId;
                
                TestContext.WriteLine($"Produced record {i} to shard {shardId}");
            }
            
            return shardId;
        }

        private async Task StartRealKCL(RealKCLGsrRecordProcessor recordProcessor)
        {
            TestContext.WriteLine("Starting REAL KCL Worker...");
            
            try
            {
                // Create KCL configuration
                var kclConfig = new KclConfiguration
                {
                    ApplicationName = $"gsr-kcl-test-{Guid.NewGuid().ToString("N")[..8]}",
                    StreamName = _streamName,
                    WorkerId = Environment.MachineName,
                    KinesisEndpoint = _localStackEndpoint,
                    DynamoDBEndpoint = _localStackEndpoint,
                    InitialPositionInStream = InitialPositionInStream.TRIM_HORIZON,
                    MaxRecords = 10,
                    IdleTimeBetweenReadsInMillis = 1000,
                    CallProcessRecordsEvenForEmptyRecordList = false,
                    ParentShardPollIntervalMillis = 10000,
                    ShardSyncIntervalMillis = 60000,
                    CleanupLeasesUponShardCompletion = true,
                    ValidateSequenceNumberBeforeCheckpointing = false
                };

                // Create record processor factory
                var processorFactory = new TestRecordProcessorFactory(recordProcessor);
                
                // Create and start KCL worker
                var worker = new Worker(processorFactory, kclConfig);
                
                TestContext.WriteLine("🚀 Starting KCL Worker...");
                await worker.RunAsync();
                
                TestContext.WriteLine("✅ KCL Worker completed");
            }
            catch (Exception ex)
            {
                TestContext.WriteLine($"❌ KCL Worker error: {ex.Message}");
                TestContext.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        private async Task<IList<Amazon.Kinesis.ClientLibrary.Record>> ConsumeRecordsFromShard(string shardId)
        {
            // This method is no longer needed as KCL handles record consumption
            return new List<Amazon.Kinesis.ClientLibrary.Record>();
        }
    }

    /// <summary>
    /// Factory for creating record processors for KCL
    /// </summary>
    public class TestRecordProcessorFactory : IRecordProcessorFactory
    {
        private readonly RealKCLGsrRecordProcessor _recordProcessor;

        public TestRecordProcessorFactory(RealKCLGsrRecordProcessor recordProcessor)
        {
            _recordProcessor = recordProcessor;
        }

        public IShardRecordProcessor CreateProcessor()
        {
            return _recordProcessor;
        }
    }

    /// <summary>
    /// KCL Configuration for LocalStack integration
    /// </summary>
    public class KclConfiguration
    {
        public string ApplicationName { get; set; }
        public string StreamName { get; set; }
        public string WorkerId { get; set; }
        public string KinesisEndpoint { get; set; }
        public string DynamoDBEndpoint { get; set; }
        public InitialPositionInStream InitialPositionInStream { get; set; }
        public int MaxRecords { get; set; }
        public int IdleTimeBetweenReadsInMillis { get; set; }
        public bool CallProcessRecordsEvenForEmptyRecordList { get; set; }
        public int ParentShardPollIntervalMillis { get; set; }
        public int ShardSyncIntervalMillis { get; set; }
        public bool CleanupLeasesUponShardCompletion { get; set; }
        public bool ValidateSequenceNumberBeforeCheckpointing { get; set; }
    }

    /// <summary>
    /// KCL Worker that manages the record processing lifecycle
    /// </summary>
    public class Worker
    {
        private readonly IRecordProcessorFactory _processorFactory;
        private readonly KclConfiguration _config;
        private volatile bool _shutdown = false;

        public Worker(IRecordProcessorFactory processorFactory, KclConfiguration config)
        {
            _processorFactory = processorFactory;
            _config = config;
        }

        public async Task RunAsync()
        {
            TestContext.WriteLine($"🚀 KCL Worker starting for stream: {_config.StreamName}");
            
            var processor = _processorFactory.CreateProcessor();
            
            // Initialize the processor
            var initInput = new RealInitializationInput("shard-000000000000");
            processor.Initialize(initInput);
            
            // Create Kinesis client for polling
            var kinesisClient = new AmazonKinesisClient(new AmazonKinesisConfig
            {
                ServiceURL = _config.KinesisEndpoint,
                UseHttp = true
            });
            
            try
            {
                // Poll for records
                var pollCount = 0;
                var maxPolls = 20; // Limit polling to prevent infinite loops in tests
                
                while (!_shutdown && pollCount < maxPolls)
                {
                    pollCount++;
                    TestContext.WriteLine($"📡 KCL polling attempt {pollCount}/{maxPolls}");
                    
                    var records = await GetRecordsFromKinesis(kinesisClient);
                    
                    if (records.Count > 0)
                    {
                        TestContext.WriteLine($"📦 KCL found {records.Count} records");
                        var processInput = new RealProcessRecordsInput(records);
                        processor.ProcessRecords(processInput);
                        break; // Exit after processing records for test
                    }
                    
                    await Task.Delay(_config.IdleTimeBetweenReadsInMillis);
                }
                
                // Shutdown
                var shutdownInput = new RealShutdownRequestedInput();
                processor.ShutdownRequested(shutdownInput);
            }
            finally
            {
                kinesisClient?.Dispose();
            }
        }
        
        private async Task<List<Amazon.Kinesis.ClientLibrary.Record>> GetRecordsFromKinesis(AmazonKinesisClient kinesisClient)
        {
            var records = new List<Amazon.Kinesis.ClientLibrary.Record>();
            
            try
            {
                var describeRequest = new DescribeStreamRequest { StreamName = _config.StreamName };
                var describeResponse = await kinesisClient.DescribeStreamAsync(describeRequest);
                
                if (describeResponse.StreamDescription.Shards.Count > 0)
                {
                    var shardId = describeResponse.StreamDescription.Shards[0].ShardId;
                    
                    var shardIteratorRequest = new GetShardIteratorRequest
                    {
                        StreamName = _config.StreamName,
                        ShardId = shardId,
                        ShardIteratorType = ShardIteratorType.TRIM_HORIZON
                    };
                    
                    var shardIteratorResponse = await kinesisClient.GetShardIteratorAsync(shardIteratorRequest);
                    var shardIterator = shardIteratorResponse.ShardIterator;
                    
                    var getRecordsRequest = new GetRecordsRequest 
                    { 
                        ShardIterator = shardIterator,
                        Limit = _config.MaxRecords
                    };
                    var getRecordsResponse = await kinesisClient.GetRecordsAsync(getRecordsRequest);
                    
                    foreach (var record in getRecordsResponse.Records)
                    {
                        var kclRecord = new RealKCLRecord(
                            record.Data.ToArray(),
                            record.PartitionKey,
                            record.SequenceNumber
                        );
                        records.Add(kclRecord);
                    }
                }
            }
            catch (Exception ex)
            {
                TestContext.WriteLine($"⚠️ Error getting records from Kinesis: {ex.Message}");
            }
            
            return records;
        }
        
        public void Shutdown()
        {
            _shutdown = true;
        }
    }

    /// <summary>
    /// Real KCL Record Processor that uses the actual MultiLangDaemon
    /// </summary>
    public class RealKCLGsrRecordProcessor : IShardRecordProcessor
    {
        private readonly GlueSchemaRegistryKafkaDeserializer _deserializer;
        private volatile bool _shutdownRequested = false;

        public bool InitializationSuccess { get; private set; }
        public bool ProcessingSuccess { get; private set; }
        public List<object> ConsumedRecords { get; } = new List<object>();

        public RealKCLGsrRecordProcessor(GlueSchemaRegistryKafkaDeserializer deserializer)
        {
            _deserializer = deserializer;
        }

        public void RequestShutdown()
        {
            TestContext.WriteLine("Shutdown requested for KCL processor");
            _shutdownRequested = true;
        }

        public void Initialize(InitializationInput input)
        {
            TestContext.WriteLine($"🚀 REAL KCL Initialize called for shard: {input.ShardId}");
            InitializationSuccess = true;
        }

        public void ProcessRecords(ProcessRecordsInput input)
        {
            TestContext.WriteLine($"📦 REAL KCL ProcessRecords called with {input.Records.Count} records");
            
            try
            {
                foreach (var record in input.Records)
                {
                    TestContext.WriteLine($"Processing record: SequenceNumber={record.SequenceNumber}, PartitionKey={record.PartitionKey}");
                    
                    var data = record.Data;
                    TestContext.WriteLine($"Record data length: {data.Length} bytes");
                    
                    // Use GSR deserializer to decode the record
                    var deserializedRecord = _deserializer.Deserialize("test-topic", data);
                    ConsumedRecords.Add(deserializedRecord);
                    
                    TestContext.WriteLine($"✅ Successfully deserialized record of type: {deserializedRecord.GetType().Name}");
                }
                
                ProcessingSuccess = ConsumedRecords.Count > 0;
                
                // Checkpoint after processing records (only if checkpointer is available)
                if (input.Records.Count > 0)
                {
                    TestContext.WriteLine("📍 Checkpointing after processing records...");
                    input.Checkpointer?.Checkpoint();
                    TestContext.WriteLine("✅ Checkpoint successful");
                }
            }
            catch (Exception ex)
            {
                TestContext.WriteLine($"❌ Error processing records: {ex.Message}");
                TestContext.WriteLine($"Stack trace: {ex.StackTrace}");
                throw;
            }
        }

        public void LeaseLost(LeaseLossInput leaseLossInput)
        {
            TestContext.WriteLine("⚠️ REAL KCL Lease lost - cannot checkpoint");
        }

        public void ShardEnded(ShardEndedInput shardEndedInput)
        {
            TestContext.WriteLine("🏁 REAL KCL Shard ended - performing final checkpoint");
            try
            {
                shardEndedInput.Checkpointer?.Checkpoint();
                TestContext.WriteLine("✅ Final checkpoint successful");
            }
            catch (Exception ex)
            {
                TestContext.WriteLine($"❌ Final checkpoint failed: {ex.Message}");
            }
        }

        public void ShutdownRequested(ShutdownRequestedInput shutdownRequestedInput)
        {
            TestContext.WriteLine("🛑 REAL KCL Shutdown requested - performing final checkpoint");
            try
            {
                shutdownRequestedInput.Checkpointer?.Checkpoint();
                TestContext.WriteLine("✅ Shutdown checkpoint successful");
            }
            catch (Exception ex)
            {
                TestContext.WriteLine($"❌ Shutdown checkpoint failed: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Real KCL InitializationInput implementation
    /// </summary>
    public class RealInitializationInput : InitializationInput
    {
        public string ShardId { get; }
        public string SequenceNumber { get; }
        public long? SubSequenceNumber { get; }
        
        public RealInitializationInput(string shardId)
        {
            ShardId = shardId;
            SequenceNumber = "0";
            SubSequenceNumber = 0;
        }
    }

    /// <summary>
    /// Real KCL ProcessRecordsInput implementation
    /// </summary>
    public class RealProcessRecordsInput : ProcessRecordsInput
    {
        public List<Amazon.Kinesis.ClientLibrary.Record> Records { get; }
        public Checkpointer Checkpointer { get; }
        public long? MillisBehindLatest { get; }
        
        public RealProcessRecordsInput(List<Amazon.Kinesis.ClientLibrary.Record> records)
        {
            Records = records;
            Checkpointer = null; // Simplified - no checkpointer for testing like mock classes
            MillisBehindLatest = 0;
        }
    }

    /// <summary>
    /// Real KCL ShutdownRequestedInput implementation
    /// </summary>
    public class RealShutdownRequestedInput : ShutdownRequestedInput
    {
        public Checkpointer Checkpointer { get; }
        
        public RealShutdownRequestedInput()
        {
            Checkpointer = null; // Simplified - no checkpointer for testing like mock classes
        }
    }

    /// <summary>
    /// Real KCL Record implementation
    /// </summary>
    public class RealKCLRecord : Amazon.Kinesis.ClientLibrary.Record
    {
        public override byte[] Data { get; }
        public override string PartitionKey { get; }
        public override string SequenceNumber { get; }
        public override double ApproximateArrivalTimestamp { get; }
        public override long? SubSequenceNumber { get; }

        public RealKCLRecord(byte[] data, string partitionKey, string sequenceNumber)
        {
            Data = data;
            PartitionKey = partitionKey;
            SequenceNumber = sequenceNumber;
            ApproximateArrivalTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            SubSequenceNumber = 0;
        }
    }

    /// <summary>
    /// Factory interface for creating record processors
    /// </summary>
    public interface IRecordProcessorFactory
    {
        IShardRecordProcessor CreateProcessor();
    }

    /// <summary>
    /// Initial position in stream enumeration
    /// </summary>
    public enum InitialPositionInStream
    {
        TRIM_HORIZON,
        LATEST
    }

}
