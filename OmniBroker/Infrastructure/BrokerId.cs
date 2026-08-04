namespace OmniBroker.Infrastructure;

public sealed record BrokerId
{
    public Guid Id { get; set; } = Guid.NewGuid();
}
