using Xunit;
using CurtisLawhorn.ApiDataStreaming;
using CurtisLawhorn.ApiDataStreaming.Configuration;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text;

namespace CurtisLawhorn.ApiDataStreaming.Tests;

public class ApiDataStreamingPublisherTests 
{
    private readonly Mock<RequestDelegate> _nextMock;
    private readonly Mock<ILogger<ApiDataStreamPublisher>> _loggerMock;
    private readonly ApiDataStreamingConfiguration _config;
    private readonly Mock<IAmazonKinesis> _kinesisMock;
    private readonly ApiDataStreamPublisher _publisher;

    public ApiDataStreamingPublisherTests()
    {
        _nextMock = new Mock<RequestDelegate>();
        _loggerMock = new Mock<ILogger<ApiDataStreamPublisher>>();
        _config = new ApiDataStreamingConfiguration { StreamName = "TestStream" };
        _kinesisMock = new Mock<IAmazonKinesis>();
        _publisher = new ApiDataStreamPublisher(
            _nextMock.Object,
            _loggerMock.Object,
            _config,
            _kinesisMock.Object
        );
    }

    [Fact]
    public async Task InvokeAsync_CallsNextDelegateAndStreamsToKinesis()
    {
        // Arrange
        var context = new DefaultHttpContext();
        context.Request.Method = "GET";
        context.Request.Path = "/test";
        context.Response.Body = new MemoryStream();
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("request-body"));

        _nextMock.Setup(n => n(It.IsAny<HttpContext>())).Returns(Task.CompletedTask);

        _kinesisMock
            .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default))
            .ReturnsAsync(new PutRecordResponse());

        // Act
        await _publisher.InvokeAsync(context);

        // Assert
        _nextMock.Verify(n => n(It.IsAny<HttpContext>()), Times.Once);
        _kinesisMock.Verify(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default), Times.Once);
    }

    [Fact]
    public async Task InvokeAsync_LogsErrorOnException()
    {
        // Arrange
        var context = new DefaultHttpContext();
        context.Request.Method = "POST";
        context.Request.Path = "/error";
        context.Response.Body = new MemoryStream();
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("request-body"));

        _nextMock.Setup(n => n(It.IsAny<HttpContext>())).Throws(new Exception("Test exception"));

        // Act
        await _publisher.InvokeAsync(context);

        // Assert
        _loggerMock.Verify(
            l => l.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString().Contains("Failed to stream API data to Kinesis.")),
                It.IsAny<Exception>(),
                It.IsAny<Func<It.IsAnyType, Exception, string>>()),
            Times.Once);
    }

    [Fact]
    public async Task StreamToKinesisAsync_SendsCorrectData()
    {
        // Arrange
        var requestData = new { Method = "GET", Path = "/unit", Body = "body" };
        var responseData = new { StatusCode = 200, Body = "response" };

        PutRecordRequest capturedRequest = null;
        _kinesisMock
            .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default))
            .Callback<PutRecordRequest, System.Threading.CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new PutRecordResponse());

        // Act
        var streamToKinesisAsync = typeof(ApiDataStreamPublisher)
            .GetMethod("StreamToKinesisAsync", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);

        await (Task)streamToKinesisAsync.Invoke(_publisher, new object[] { requestData, responseData });

        // Assert
        Assert.NotNull(capturedRequest);
        Assert.Equal(_config.StreamName, capturedRequest.StreamName);

        capturedRequest.Data.Seek(0, SeekOrigin.Begin);
        var json = await new StreamReader(capturedRequest.Data).ReadToEndAsync();
        Assert.Contains("\"Request\"", json);
        Assert.Contains("\"Response\"", json);
    }

    [Fact]
    public async Task InvokeAsync_CapturesRequestHeaders()
    {
        // Arrange
        var context = new DefaultHttpContext();
        context.Request.Method = "GET";
        context.Request.Path = "/header-test";
        context.Response.Body = new MemoryStream();
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("body"));
        context.Request.Headers["X-Test-Header"] = "HeaderValue";

        _nextMock.Setup(n => n(It.IsAny<HttpContext>())).Returns(Task.CompletedTask);
        _kinesisMock
            .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default))
            .ReturnsAsync(new PutRecordResponse());

        PutRecordRequest capturedRequest = null;
        _kinesisMock
            .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default))
            .Callback<PutRecordRequest, System.Threading.CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new PutRecordResponse());

        // Act
        await _publisher.InvokeAsync(context);

        // Assert
        Assert.NotNull(capturedRequest);
        capturedRequest.Data.Seek(0, SeekOrigin.Begin);
        var json = await new StreamReader(capturedRequest.Data).ReadToEndAsync();
        Assert.Contains("\"X-Test-Header\":\"HeaderValue\"", json);
    }

    [Fact]
    public async Task InvokeAsync_CapturesResponseHeaders()
    {
        // Arrange
        var context = new DefaultHttpContext();
        context.Request.Method = "GET";
        context.Request.Path = "/header-test";
        context.Response.Body = new MemoryStream();
        context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("body"));

        _nextMock.Setup(n => n(It.IsAny<HttpContext>()))
            .Callback<HttpContext>(ctx => ctx.Response.Headers["X-Response-Header"] = "ResponseValue")
            .Returns(Task.CompletedTask);

        PutRecordRequest capturedRequest = null;
        _kinesisMock
            .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default))
            .Callback<PutRecordRequest, System.Threading.CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(new PutRecordResponse());

        // Act
        await _publisher.InvokeAsync(context);

        // Assert
        Assert.NotNull(capturedRequest);
        capturedRequest.Data.Seek(0, SeekOrigin.Begin);
        var json = await new StreamReader(capturedRequest.Data).ReadToEndAsync();
        Assert.Contains("\"X-Response-Header\":\"ResponseValue\"", json);
    }
}