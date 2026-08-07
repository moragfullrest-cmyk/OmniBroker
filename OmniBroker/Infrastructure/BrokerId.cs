namespace OmniBroker.Infrastructure;

public sealed record BrokerId
{
    public Guid Id { get; init; } = Guid.NewGuid();
}
