namespace OmniBroker.Infrastructure;

/// <summary>
/// Broker instance identifier in DI
/// </summary>
public sealed record BrokerId()
{
    /// <summary>
    /// Unique identifier
    /// </summary>
    public Guid Id { get; init; } = Guid.NewGuid();
}
