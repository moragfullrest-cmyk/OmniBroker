using Confluent.Kafka;

namespace OmniBroker.Kafka.ServiceSetup;

public class KafkaSettings
{
    public required string Hosts { get; init; }
    public string? ClientId { get; init; }
    public string? GroupId { get; init; }
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Earliest;
    public SecurityProtocol? SecurityProtocol { get; init; }
    public string? SaslUsername { get; init; }
    public string? SaslPassword { get; init; }
    public SaslMechanism? SaslMechanism { get; init; }
}
