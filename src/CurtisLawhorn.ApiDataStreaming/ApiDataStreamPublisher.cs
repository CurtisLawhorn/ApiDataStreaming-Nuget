using CurtisLawhorn.ApiDataStreaming.Configuration;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;

namespace CurtisLawhorn.ApiDataStreaming;

public class ApiDataStreamPublisher
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ApiDataStreamPublisher> _logger;
    private readonly ApiDataStreamingConfiguration _apiDataStreamingConfiguration;
    private readonly IAmazonKinesis _amazonKinesis;

    public ApiDataStreamPublisher(RequestDelegate next,
                                  ILogger<ApiDataStreamPublisher> logger,
                                  ApiDataStreamingConfiguration apiDataStreamingConfiguration,
                                  IAmazonKinesis amazonKinesis)
    {
        _next = next;
        _logger = logger;
        _apiDataStreamingConfiguration = apiDataStreamingConfiguration;
        _amazonKinesis = amazonKinesis;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {

            var requestData = await CaptureRequestAsync(context);

            var originalBodyStream = context.Response.Body;
            using var responseBodyStream = new MemoryStream();
            context.Response.Body = responseBodyStream;

            await _next(context);

            var responseData = await CaptureResponseAsync(context, responseBodyStream);

            await responseBodyStream.CopyToAsync(originalBodyStream);

            _ = Task.Run(() => StreamToKinesisAsync(requestData, responseData));

        }

        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to stream API data to Kinesis.");
        }
    }

    private static async Task<object> CaptureRequestAsync(HttpContext context)
    {
        context.Request.EnableBuffering();
        var body = await new StreamReader(context.Request.Body).ReadToEndAsync();
        context.Request.Body.Position = 0;

        return new
        {
            context.Request.Method,
            Path = context.Request.Path.Value,
            QueryString = context.Request.QueryString.Value,
            Headers = context.Request.Headers.ToDictionary(h => h.Key, h => h.Value.ToString()),
            Body = body,
            Timestamp = DateTime.UtcNow
        };
    }

    private static async Task<object> CaptureResponseAsync(HttpContext context, MemoryStream responseBodyStream)
    {
        responseBodyStream.Seek(0, SeekOrigin.Begin);
        var responseBody = await new StreamReader(responseBodyStream).ReadToEndAsync();
        responseBodyStream.Seek(0, SeekOrigin.Begin);

        return new
        {
            context.Response.StatusCode,
            Headers = context.Response.Headers.ToDictionary(h => h.Key, h => h.Value.ToString()),
            Body = responseBody,
            Timestamp = DateTime.UtcNow
        };
    }

    private void StreamToKinesisAsync(object requestData, object responseData)
    {
        var apiData = new { Request = requestData, Response = responseData };
        var json = JsonSerializer.Serialize(apiData);
        var data = Encoding.UTF8.GetBytes(json);

        var request = new PutRecordRequest
        {
            StreamName = _apiDataStreamingConfiguration.StreamName,
            Data = new MemoryStream(data),
            PartitionKey = Guid.NewGuid().ToString()
        };

        // Fire-and-forget: do not await, log any exceptions
        _ = _amazonKinesis.PutRecordAsync(request).ContinueWith(task =>
        {
            if (task.Exception != null)
            {
                _logger.LogError(task.Exception, "Failed to put record to Kinesis.");
            }
        }, TaskContinuationOptions.OnlyOnFaulted);
    }

}