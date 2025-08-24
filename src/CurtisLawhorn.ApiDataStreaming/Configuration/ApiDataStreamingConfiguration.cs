using System.Diagnostics.CodeAnalysis;

namespace CurtisLawhorn.ApiDataStreaming.Configuration;

[ExcludeFromCodeCoverage]
public class ApiDataStreamingConfiguration
{
    public required string StreamName { get; set; }
}