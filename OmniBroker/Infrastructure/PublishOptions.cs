namespace OmniBroker.Infrastructure;

public sealed class PublishOptions
{
    public string? CorrelationId { get; init; }
    public string? ReplyTo { get; init; }
    public string? Destination { get; init; }
}
