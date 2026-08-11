namespace OmniBroker.Infrastructure;

/// <summary>
/// Message publish options
/// </summary>
public sealed record PublishOptions(string? CorrelationId, string? ReplyTo, string? Destination);
