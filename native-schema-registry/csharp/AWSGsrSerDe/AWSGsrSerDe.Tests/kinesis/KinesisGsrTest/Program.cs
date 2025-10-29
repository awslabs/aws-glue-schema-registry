using System;
using System.Threading.Tasks;

namespace KinesisGsrTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting Kinesis GSR Integration Test...");
            
            try
            {
                var test = new KinesisGsrIntegrationTest();
                await test.RunTest();
                Console.WriteLine("✓ Test completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Test failed: {ex.Message}");
                Environment.Exit(1);
            }
        }
    }
}
