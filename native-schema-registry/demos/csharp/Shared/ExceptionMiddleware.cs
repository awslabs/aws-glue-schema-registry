using System;
using System.Threading.Tasks;
using KafkaFlow;
using KafkaFlow.Middlewares;

public class ExceptionMiddleware : IMessageMiddleware
{
    public async Task Invoke(IMessageContext context, MiddlewareDelegate next)
    {
        try
        {
            // Call the next middleware in the pipeline (e.g., serializer)
            await next(context);
        }
        catch (Exception ex)
        {
            // Log the error or rethrow it to fail fast
            Console.WriteLine($"SerDes error for topic '{context.Message}': {ex}");

            // If you want the Produce call to throw:
            throw;
        }
    }
}