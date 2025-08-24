using Amazon.Kinesis;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics.CodeAnalysis;

namespace CurtisLawhorn.ApiDataStreaming.Configuration;

[ExcludeFromCodeCoverage]
public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddApiDataStreaming(this IServiceCollection services, IConfiguration configuration)
    {
        var apiDataStreamingConfiguration =
            configuration.GetSection($"AWS:{nameof(ApiDataStreamingConfiguration)}").Get<ApiDataStreamingConfiguration>()
                ?? throw new Exception($"{nameof(ApiDataStreamingConfiguration)} not provided.");
        
        // Used by the ApiDataStreamPublisher
        services.AddSingleton(apiDataStreamingConfiguration);
                    
        // Uses the AWS SDK's default configuration
        // For example, it automatically knows the region where resources are deployed/running
        services.AddAWSService<IAmazonKinesis>();

        // Deprecated in favor of using AddAWSService above
        /* 
        services.AddSingleton<IAmazonKinesis>(sp =>
        {
            var config = sp.GetRequiredService<ApiDataStreamingConfiguration>();
            var regionEndpoint = Amazon.RegionEndpoint.GetBySystemName(apiDataStreamingConfiguration.Region);
            var kinesisConfig = new AmazonKinesisConfig { RegionEndpoint = regionEndpoint };
            return new AmazonKinesisClient(kinesisConfig);
        });
        */

        return services;
    }        
}

[ExcludeFromCodeCoverage]
public static class ApplicationBuilderExtensions
{
    public static IApplicationBuilder UseApiDataStreaming(this IApplicationBuilder builder)
    {
        return builder.UseMiddleware<ApiDataStreamPublisher>();
    }
}