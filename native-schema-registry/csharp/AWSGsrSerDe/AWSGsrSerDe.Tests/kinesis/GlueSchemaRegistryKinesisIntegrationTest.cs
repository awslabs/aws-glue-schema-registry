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
    public class GlueSchemaRegistryKinesisIntegrationTest
    {
        private readonly string _localStackEndpoint = Environment.GetEnvironmentVariable("LOCALSTACK_ENDPOINT") ?? "http://localhost:4566";
        private string _streamName;
        private AmazonKinesisClient _kinesisClient;
        private readonly List<string> _schemasToCleanUp = new List<string>();

        private static readonly string AVRO_CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal.properties");
        private static readonly string JSON_CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal-json.properties");
        private static readonly string PROTOBUF_CONFIG_PATH = GetConfigPath("configuration/test-configs/valid-minimal-protobuf.properties");

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

        /// <summary>
        /// Finds the project root by looking for .csproj file and returns absolute path to config file
        /// </summary>
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
            _streamName = $"gsr-integ-test-kinesis-stream-{Guid.NewGuid().ToString("N")[..8]}";
            
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
            
            // Clean up schemas
            foreach (var schemaName in _schemasToCleanUp)
            {
                TestContext.WriteLine($"Cleaning up schema: {schemaName}");
                // Schema cleanup logic would go here in real implementation
            }
        }

        [Test]
        public async Task TestKinesisBasicProduceConsume()
        {
            TestContext.WriteLine("Starting basic Kinesis produce/consume test...");
            
            var message = "Hello World";
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            
            var putRequest = new PutRecordRequest
            {
                StreamName = _streamName,
                PartitionKey = timestamp.ToString(),
                Data = new MemoryStream(Encoding.UTF8.GetBytes(message))
            };
            
            var putResponse = await _kinesisClient.PutRecordAsync(putRequest);
            Assert.IsNotNull(putResponse.ShardId);
            
            var records = await ConsumeRecordsFromShard(putResponse.ShardId);
            
            Assert.AreEqual(1, records.Count);
            var receivedMessage = Encoding.UTF8.GetString(records[0].Data.ToArray());
            Assert.AreEqual(message, receivedMessage);
            
            TestContext.WriteLine("✓ Basic Kinesis operations successful!");
        }

        [Test]
        public async Task TestKinesisProduceConsumeWithGSR_AVRO()
        {
            TestContext.WriteLine("Testing Kinesis with GSR - AVRO format");
            
            var serializer = new GlueSchemaRegistryKafkaSerializer(AVRO_CONFIG_PATH);
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(AVRO_CONFIG_PATH);
            
            var avroRecord = RecordGenerator.GetTestAvroRecord();
            var testRecords = new List<GenericRecord> { avroRecord };
            
            var shardId = await ProduceRecordsWithGSR(testRecords, serializer, "avro-test");
            var consumedRecords = await ConsumeRecordsWithGSR(shardId, deserializer, "avro-test");
            
            Assert.AreEqual(testRecords.Count, consumedRecords.Count);
            Assert.IsTrue(consumedRecords[0] is GenericRecord);
            
            var consumedRecord = (GenericRecord)consumedRecords[0];
            Assert.AreEqual(avroRecord, consumedRecord);
            
            TestContext.WriteLine("✓ AVRO GSR Kinesis integration test successful!");
        }

        [Test]
        public async Task TestKinesisProduceConsumeWithGSR_JSON()
        {
            TestContext.WriteLine("Testing Kinesis with GSR - JSON format");
            
            var serializer = new GlueSchemaRegistryKafkaSerializer(JSON_CONFIG_PATH);
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(JSON_CONFIG_PATH);
            
            var jsonRecord = RecordGenerator.GetSampleJsonTestData();
            var testRecords = new List<JsonDataWithSchema> { jsonRecord };
            
            var shardId = await ProduceRecordsWithGSR(testRecords, serializer, "json-test");
            var consumedRecords = await ConsumeRecordsWithGSR(shardId, deserializer, "json-test");
            
            Assert.AreEqual(testRecords.Count, consumedRecords.Count);
            Assert.IsTrue(consumedRecords[0] is JsonDataWithSchema);
            
            var consumedRecord = (JsonDataWithSchema)consumedRecords[0];
            Assert.AreEqual(jsonRecord.Schema, consumedRecord.Schema);
            Assert.AreEqual(jsonRecord.Payload, consumedRecord.Payload);
            
            TestContext.WriteLine("✓ JSON GSR Kinesis integration test successful!");
        }

        [Test]
        public async Task TestKinesisProduceConsumeWithGSR_JSONObject()
        {
            TestContext.WriteLine("Testing Kinesis with GSR - JSON Object format");
            
            var serializer = new GlueSchemaRegistryKafkaSerializer(JSON_CONFIG_PATH);
            
            // Use dataConfig for JSON object deserialization
            var dataConfig = new AWSGsrSerDe.common.GlueSchemaRegistryDataFormatConfiguration(new Dictionary<string, dynamic>
            {
                { GlueSchemaRegistryConstants.JsonObjectType, typeof(Car) }
            });
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(JSON_CONFIG_PATH, dataConfig);
            
            var testRecords = new List<Car> { SPECIFIC_TEST_RECORD };
            
            var shardId = await ProduceRecordsWithGSR(testRecords, serializer, "json-object-test");
            var consumedRecords = await ConsumeRecordsWithGSR(shardId, deserializer, "json-object-test");
            
            Assert.AreEqual(testRecords.Count, consumedRecords.Count);
            Assert.IsTrue(consumedRecords[0] is Car);
            
            var consumedRecord = (Car)consumedRecords[0];
            Assert.AreEqual(SPECIFIC_TEST_RECORD.make, consumedRecord.make);
            Assert.AreEqual(SPECIFIC_TEST_RECORD.model, consumedRecord.model);
            Assert.AreEqual(SPECIFIC_TEST_RECORD.used, consumedRecord.used);
            Assert.AreEqual(SPECIFIC_TEST_RECORD.miles, consumedRecord.miles);
            
            TestContext.WriteLine("✓ JSON Object GSR Kinesis integration test successful!");
        }

        [Test]
        public async Task TestKinesisWithKCL_AVRO()
        {
            TestContext.WriteLine("Testing KCL integration with GSR - AVRO format");
            
            var serializer = new GlueSchemaRegistryKafkaSerializer(AVRO_CONFIG_PATH);
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(AVRO_CONFIG_PATH);
            
            var avroRecord = RecordGenerator.GetTestAvroRecord();
            var testRecords = new List<GenericRecord> { avroRecord };
            
            var recordProcessor = new GsrRecordProcessor(deserializer);
            
            // Start KCL consumer simulation
            var kclTask = StartKCLConsumerSimulation(recordProcessor);
            
            // Give KCL time to initialize
            await Task.Delay(TimeSpan.FromSeconds(2));
            
            // Produce records
            await ProduceRecordsWithGSR(testRecords, serializer, "kcl-avro-test");
            
            // Wait for consumption
            await Task.Delay(TimeSpan.FromSeconds(3));
            
            Assert.IsTrue(recordProcessor.CreationSuccess);
            Assert.IsTrue(recordProcessor.ConsumptionSuccess);
            Assert.AreEqual(testRecords.Count, recordProcessor.ConsumedRecords.Count);
            
            TestContext.WriteLine("✓ KCL AVRO integration test successful!");
        }

        [Test]
        public async Task TestKinesisWithKCL_JSON()
        {
            TestContext.WriteLine("Testing KCL integration with GSR - JSON format");
            
            var serializer = new GlueSchemaRegistryKafkaSerializer(JSON_CONFIG_PATH);
            var deserializer = new GlueSchemaRegistryKafkaDeserializer(JSON_CONFIG_PATH);
            
            var jsonRecord = RecordGenerator.GetSampleJsonTestData();
            var testRecords = new List<JsonDataWithSchema> { jsonRecord };
            
            var recordProcessor = new GsrRecordProcessor(deserializer);
            
            // Start KCL consumer simulation
            var kclTask = StartKCLConsumerSimulation(recordProcessor);
            
            // Give KCL time to initialize
            await Task.Delay(TimeSpan.FromSeconds(2));
            
            // Produce records
            await ProduceRecordsWithGSR(testRecords, serializer, "kcl-json-test");
            
            // Wait for consumption
            await Task.Delay(TimeSpan.FromSeconds(3));
            
            Assert.IsTrue(recordProcessor.CreationSuccess);
            Assert.IsTrue(recordProcessor.ConsumptionSuccess);
            Assert.AreEqual(testRecords.Count, recordProcessor.ConsumedRecords.Count);
            
            TestContext.WriteLine("✓ KCL JSON integration test successful!");
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
                
                // Wait for stream to be active
                var maxWaitTime = TimeSpan.FromSeconds(30);
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
                    await Task.Delay(1000);
                }
                
                throw new Exception("Stream did not become active within timeout");
            }
            catch (ResourceInUseException)
            {
                TestContext.WriteLine($"Stream {_streamName} already exists");
            }
        }

        private async Task<string> ProduceRecordsWithGSR<T>(
            List<T> records,
            GlueSchemaRegistryKafkaSerializer serializer,
            string topicName)
        {
            string shardId = null;
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            
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
            }
            
            return shardId;
        }

        private async Task<List<Amazon.Kinesis.Model.Record>> ConsumeRecordsFromShard(string shardId)
        {
            var shardIteratorRequest = new GetShardIteratorRequest
            {
                StreamName = _streamName,
                ShardId = shardId,
                ShardIteratorType = ShardIteratorType.TRIM_HORIZON
            };
            
            var shardIteratorResponse = await _kinesisClient.GetShardIteratorAsync(shardIteratorRequest);
            
            var getRecordsRequest = new GetRecordsRequest
            {
                ShardIterator = shardIteratorResponse.ShardIterator
            };
            
            var getRecordsResponse = await _kinesisClient.GetRecordsAsync(getRecordsRequest);
            return getRecordsResponse.Records;
        }

        private async Task<List<object>> ConsumeRecordsWithGSR(
            string shardId,
            GlueSchemaRegistryKafkaDeserializer deserializer,
            string topicName)
        {
            var records = await ConsumeRecordsFromShard(shardId);
            var deserializedRecords = new List<object>();
            
            foreach (var record in records)
            {
                var data = record.Data.ToArray();
                var deserializedData = deserializer.Deserialize(topicName, data);
                deserializedRecords.Add(deserializedData);
            }
            
            return deserializedRecords;
        }

        private async Task StartKCLConsumerSimulation(GsrRecordProcessor recordProcessor)
        {
            // Simplified KCL simulation - in real implementation, use proper KCL configuration
            await Task.Run(async () =>
            {
                // Create mock initialization input since InitializationInput is abstract
                var mockInitInput = new MockInitializationInput("shard-000000000000");
                recordProcessor.Initialize(mockInitInput);
                
                // Simulate processing delay
                await Task.Delay(1000);
                
                // Simulate record processing by reading from Kinesis
                try
                {
                    var describeRequest = new DescribeStreamRequest { StreamName = _streamName };
                    var response = await _kinesisClient.DescribeStreamAsync(describeRequest);
                    
                    if (response.StreamDescription.Shards.Count > 0)
                    {
                        var shardId = response.StreamDescription.Shards[0].ShardId;
                        var records = await ConsumeRecordsFromShard(shardId);
                        
                        if (records.Count > 0)
                        {
                            // Convert Kinesis Model Records to KCL Records
                            var kclRecords = records.Select(r => new MockKCLRecord(
                                r.Data.ToArray(), 
                                r.PartitionKey, 
                                r.SequenceNumber)).Cast<Amazon.Kinesis.ClientLibrary.Record>().ToList();
                            
                            // Create mock process records input since ProcessRecordsInput is abstract
                            var mockProcessInput = new MockProcessRecordsInput(kclRecords);
                            recordProcessor.ProcessRecords(mockProcessInput);
                        }
                    }
                }
                catch (Exception ex)
                {
                    TestContext.WriteLine($"KCL simulation error: {ex.Message}");
                }
            });
        }
    }


}
