using System;
using System.Text;
using System.Threading.Tasks;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;

namespace KinesisTestApp
{
    class Program
    {
        private static readonly string StreamName = "gsr-test-stream";
        private static readonly string LocalStackEndpoint = "http://localhost:4566";
        
        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Kinesis GSR Test...");
            
            var config = new AmazonKinesisConfig
            {
                ServiceURL = LocalStackEndpoint,
                UseHttp = true
            };
            
            using var kinesisClient = new AmazonKinesisClient(config);
            
            try
            {
                // Create stream
                await CreateStream(kinesisClient);
                
                // Produce record
                await ProduceRecord(kinesisClient);
                
                // Consume record
                await ConsumeRecord(kinesisClient);
                
                Console.WriteLine("Test completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Test failed: {ex.Message}");
                Environment.Exit(1);
            }
        }
        
        private static async Task CreateStream(AmazonKinesisClient client)
        {
            try
            {
                await client.CreateStreamAsync(new CreateStreamRequest
                {
                    StreamName = StreamName,
                    ShardCount = 1
                });
                Console.WriteLine($"Created stream: {StreamName}");
                
                // Wait for stream to be active
                await Task.Delay(2000);
            }
            catch (ResourceInUseException)
            {
                Console.WriteLine($"Stream {StreamName} already exists");
            }
        }
        
        private static async Task ProduceRecord(AmazonKinesisClient client)
        {
            var data = Encoding.UTF8.GetBytes("Hello Kinesis!");
            
            var request = new PutRecordRequest
            {
                StreamName = StreamName,
                PartitionKey = "test-key",
                Data = new System.IO.MemoryStream(data)
            };
            
            var response = await client.PutRecordAsync(request);
            Console.WriteLine($"Produced record to shard: {response.ShardId}");
        }
        
        private static async Task ConsumeRecord(AmazonKinesisClient client)
        {
            // Get shard iterator
            var shardRequest = new GetShardIteratorRequest
            {
                StreamName = StreamName,
                ShardId = "shardId-000000000000",
                ShardIteratorType = ShardIteratorType.TRIM_HORIZON
            };
            
            var shardResponse = await client.GetShardIteratorAsync(shardRequest);
            
            // Get records
            var recordsRequest = new GetRecordsRequest
            {
                ShardIterator = shardResponse.ShardIterator
            };
            
            var recordsResponse = await client.GetRecordsAsync(recordsRequest);
            
            foreach (var record in recordsResponse.Records)
            {
                var data = Encoding.UTF8.GetString(record.Data.ToArray());
                Console.WriteLine($"Consumed record: {data}");
            }
        }
    }
}
