namespace OmniBroker.Infrastructure;

/// <summary>
/// Идентификатор экземпляра брокера в DI
/// </summary>
public sealed record BrokerId
{
    /// <summary>
    /// Уникальный идентификатор
    /// </summary>
    public Guid Id { get; init; } = Guid.NewGuid();
}
