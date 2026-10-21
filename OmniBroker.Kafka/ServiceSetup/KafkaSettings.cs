using Confluent.Kafka;

namespace OmniBroker.Kafka.ServiceSetup;

/// <summary>
/// Kafka connection settings
/// </summary>
public sealed class KafkaSettings
{
    /// <summary>
    /// Bootstrap servers list
    /// </summary>
    public required string Hosts { get; init; }

    /// <summary>
    /// Client identifier
    /// </summary>
    public string? ClientId { get; init; }

    /// <summary>
    /// Consumer group identifier
    /// </summary>
    public string? GroupId { get; init; }

    /// <summary>
    /// Offset reset strategy when no stored position exists
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Earliest;

    /// <summary>
    /// Connection security protocol
    /// </summary>
    public SecurityProtocol? SecurityProtocol { get; init; }

    /// <summary>
    /// SASL user name
    /// </summary>
    public string? SaslUsername { get; init; }

    /// <summary>
    /// SASL password
    /// </summary>
    public string? SaslPassword { get; init; }

    /// <summary>
    /// SASL mechanism
    /// </summary>
    public SaslMechanism? SaslMechanism { get; init; }

    /// <summary>
    /// Optional dead-letter topic. When set, failed handler results are produced here before the original offset is committed.
    /// If that produce fails, the offset stays uncommitted and the consumer seeks back to retry the record.
    /// </summary>
    public string? DeadLetterTopic { get; init; }

    /// <summary>
    /// RPC response wait timeout.
    /// </summary>
    public TimeSpan RpcTimeout { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Optional topic where RPC replies are produced.
    /// When RPC callers are registered, startup requires this topic and claims one partition
    /// that has a leader. The claim is the consumer group "{GroupId or broker name}.rpc-reply-{partitionId}":
    /// a live member means that partition is taken. That consumer stays paused and does not read replies.
    /// A separate consumer assigns only the claimed partition and correlates replies with pending calls.
    /// </summary>
    public string? RpcReplyTopic { get; init; }
}
