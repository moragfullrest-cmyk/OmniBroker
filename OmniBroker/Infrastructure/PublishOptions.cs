namespace OmniBroker.Infrastructure;

/// <summary>
/// Options shared by every broker when publishing a message.
/// A broker package may derive its own options when the transport needs extra data.
/// </summary>
public record PublishOptions(
    string? CorrelationId,
    string? ReplyTo,
    string? Destination);
