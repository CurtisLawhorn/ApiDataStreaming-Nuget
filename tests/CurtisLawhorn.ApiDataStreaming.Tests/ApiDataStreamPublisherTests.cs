using CurtisLawhorn.ApiDataStreaming;
using CurtisLawhorn.ApiDataStreaming.Configuration;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text;
using System.Text.Json;
using Xunit;

namespace CurtisLawhorn.ApiDataStreaming.Tests
{
    public class ApiDataStreamPublisherTests
    {
        private readonly Mock<RequestDelegate> _nextMock;
        private readonly Mock<ILogger<ApiDataStreamPublisher>> _loggerMock;
        private readonly ApiDataStreamingConfiguration _config;
        private readonly Mock<IAmazonKinesis> _kinesisMock;
        private readonly ApiDataStreamPublisher _publisher;

        public ApiDataStreamPublisherTests()
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
        public async Task InvokeAsync_StreamsRequestAndResponseToKinesis()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Method = "GET";
            context.Request.Path = "/test";
            context.Request.QueryString = new QueryString("?foo=bar");
            context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("request-body"));
            context.Response.Body = new MemoryStream();

            _nextMock.Setup(n => n(It.IsAny<HttpContext>())).Returns(Task.CompletedTask);

            var tcs = new TaskCompletionSource<bool>();
            _kinesisMock
                .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default))
                .Callback(() => tcs.SetResult(true))
                .ReturnsAsync(new PutRecordResponse());

            // Act
            await _publisher.InvokeAsync(context);
            await tcs.Task; // Wait for fire-and-forget to complete

            // Assert
            _kinesisMock.Verify(k => k.PutRecordAsync(
                It.Is<PutRecordRequest>(r =>
                    r.StreamName == "TestStream" &&
                    r.Data.Length > 0 &&
                    !string.IsNullOrEmpty(r.PartitionKey)
                ),
                default
            ), Times.Once);
        }

        [Fact]
        public async Task InvokeAsync_WhenKinesisThrows_LogsError()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Method = "POST";
            context.Request.Path = "/error";
            context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("body"));
            context.Response.Body = new MemoryStream();

            _nextMock.Setup(n => n(It.IsAny<HttpContext>())).Returns(Task.CompletedTask);

            var exception = new Exception("Kinesis failure");
            var tcs = new TaskCompletionSource<bool>();

            // Return a faulted Task to trigger the ContinueWith in the middleware
            _kinesisMock
                .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), It.IsAny<System.Threading.CancellationToken>()))
                .Returns(() => Task.FromException<PutRecordResponse>(exception));

            // Accept any exception in the logger mock
            _loggerMock
                .Setup(l => l.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()
                ))
                .Callback(() => tcs.TrySetResult(true));

            // Act
            await _publisher.InvokeAsync(context);

            // Wait for logger to be called after fire-and-forget, with timeout
            var loggerCalled = await Task.WhenAny(tcs.Task, Task.Delay(5000)) == tcs.Task;
            Assert.True(loggerCalled, "Logger was not called within timeout.");

            // Assert
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    It.IsAny<Exception>(),
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()
                ),
                Times.Once
            );
        }

        [Fact]
        public async Task InvokeAsync_WithEmptyRequestBody_StreamsEmptyRequestBodyToKinesis()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Method = "POST";
            context.Request.Path = "/empty";
            context.Request.Body = new MemoryStream(); // Empty body
            context.Response.Body = new MemoryStream();

            _nextMock.Setup(n => n(It.IsAny<HttpContext>())).Returns(Task.CompletedTask);

            var tcs = new TaskCompletionSource<bool>();
            PutRecordRequest? capturedRequest = null;
            _kinesisMock
                .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default))
                .Callback<PutRecordRequest, System.Threading.CancellationToken>((req, _) => {
                    capturedRequest = req;
                    tcs.SetResult(true);
                })
                .ReturnsAsync(new PutRecordResponse());

            // Act
            await _publisher.InvokeAsync(context);
            await tcs.Task; // Wait for fire-and-forget to complete

            // Assert
            Assert.NotNull(capturedRequest);
            var json = Encoding.UTF8.GetString(capturedRequest.Data.ToArray());
            using var doc = JsonDocument.Parse(json);
            var requestBody = doc.RootElement.GetProperty("Request").GetProperty("Body").GetString();
            Assert.Equal(string.Empty, requestBody);
        }

        [Fact]
        public async Task InvokeAsync_WithNon200ResponseStatusCode_StreamsResponseWithCorrectStatusCode()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Method = "GET";
            context.Request.Path = "/notfound";
            context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("request"));
            context.Response.Body = new MemoryStream();

            // Simulate a 404 response inside the pipeline
            _nextMock.Setup(n => n(It.IsAny<HttpContext>()))
                .Returns<HttpContext>(async ctx =>
                {
                    ctx.Response.StatusCode = 404;
                    await ctx.Response.WriteAsync("Not Found");
                });

            var tcs = new TaskCompletionSource<bool>();
            PutRecordRequest? capturedRequest = null;
            _kinesisMock
                .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default))
                .Callback<PutRecordRequest, System.Threading.CancellationToken>((req, _) => {
                    capturedRequest = req;
                    tcs.SetResult(true);
                })
                .ReturnsAsync(new PutRecordResponse());

            // Act
            await _publisher.InvokeAsync(context);
            await tcs.Task;

            // Assert
            Assert.NotNull(capturedRequest);
            var json = Encoding.UTF8.GetString(capturedRequest.Data.ToArray());
            using var doc = JsonDocument.Parse(json);
            var responseStatus = doc.RootElement.GetProperty("Response").GetProperty("StatusCode").GetInt32();
            var responseBody = doc.RootElement.GetProperty("Response").GetProperty("Body").GetString();
            Assert.Equal(404, responseStatus);
            Assert.Equal("Not Found", responseBody);
        }

        [Fact]
        public async Task InvokeAsync_StreamsRequestHeadersToKinesis()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Method = "GET";
            context.Request.Path = "/headers";
            context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("body"));
            context.Response.Body = new MemoryStream();

            // Add custom headers
            context.Request.Headers["X-Test-Header"] = "HeaderValue";
            context.Request.Headers["Another-Header"] = "AnotherValue";

            _nextMock.Setup(n => n(It.IsAny<HttpContext>())).Returns(Task.CompletedTask);

            var tcs = new TaskCompletionSource<bool>();
            PutRecordRequest? capturedRequest = null;
            _kinesisMock
                .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), It.IsAny<System.Threading.CancellationToken>()))
                .Callback<PutRecordRequest, System.Threading.CancellationToken>((req, _) => {
                    capturedRequest = req;
                    tcs.TrySetResult(true);
                })
                .ReturnsAsync(new PutRecordResponse());

            // Act
            await _publisher.InvokeAsync(context);
            await tcs.Task; // Wait for fire-and-forget to complete

            // Assert
            Assert.NotNull(capturedRequest);
            var json = Encoding.UTF8.GetString(capturedRequest.Data.ToArray());
            using var doc = JsonDocument.Parse(json);
            var headers = doc.RootElement.GetProperty("Request").GetProperty("Headers");
            Assert.Equal("HeaderValue", headers.GetProperty("X-Test-Header").GetString());
            Assert.Equal("AnotherValue", headers.GetProperty("Another-Header").GetString());
        }

        [Fact]
        public async Task InvokeAsync_StreamsResponseHeadersToKinesis()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Method = "GET";
            context.Request.Path = "/response-headers";
            context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("body"));
            context.Response.Body = new MemoryStream();

            // Add custom response headers
            context.Response.Headers["X-Response-Header"] = "ResponseValue";
            context.Response.Headers["Another-Response"] = "AnotherValue";

            _nextMock.Setup(n => n(It.IsAny<HttpContext>())).Returns(Task.CompletedTask);

            var tcs = new TaskCompletionSource<bool>();
            PutRecordRequest? capturedRequest = null;
            _kinesisMock
                .Setup(k => k.PutRecordAsync(It.IsAny<PutRecordRequest>(), default))
                .Callback<PutRecordRequest, System.Threading.CancellationToken>((req, _) => {
                    capturedRequest = req;
                    tcs.SetResult(true);
                })
                .ReturnsAsync(new PutRecordResponse());

            // Act
            await _publisher.InvokeAsync(context);
            await tcs.Task; // Wait for fire-and-forget to complete

            // Assert
            Assert.NotNull(capturedRequest);
            var json = Encoding.UTF8.GetString(capturedRequest.Data.ToArray());
            using var doc = JsonDocument.Parse(json);
            var headers = doc.RootElement.GetProperty("Response").GetProperty("Headers");
            Assert.Equal("ResponseValue", headers.GetProperty("X-Response-Header").GetString());
            Assert.Equal("AnotherValue", headers.GetProperty("Another-Response").GetString());
        }

        [Fact]
        public async Task InvokeAsync_WhenExceptionThrown_LogsError()
        {
            // Arrange
            var context = new DefaultHttpContext();
            context.Request.Method = "GET";
            context.Request.Path = "/error";
            context.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes("body"));
            context.Response.Body = new MemoryStream();

            // Simulate an exception thrown by the next middleware
            var testException = new Exception("Test exception");
            _nextMock.Setup(n => n(It.IsAny<HttpContext>())).ThrowsAsync(testException);

            // Track logger invocation
            var tcs = new TaskCompletionSource<bool>();
            _loggerMock
                .Setup(l => l.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    testException,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()
                ))
                .Callback(() => tcs.TrySetResult(true));

            // Act
            await _publisher.InvokeAsync(context);
            var loggerCalled = await Task.WhenAny(tcs.Task, Task.Delay(2000)) == tcs.Task;
            Assert.True(loggerCalled, "Logger was not called within timeout.");

            // Assert
            _loggerMock.Verify(
                l => l.Log(
                    LogLevel.Error,
                    It.IsAny<EventId>(),
                    It.IsAny<It.IsAnyType>(),
                    testException,
                    It.IsAny<Func<It.IsAnyType, Exception?, string>>()
                ),
                Times.Once
            );
        }

    }
}