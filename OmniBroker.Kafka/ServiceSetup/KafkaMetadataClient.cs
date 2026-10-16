namespace OmniBroker.Kafka.ServiceSetup;

internal sealed record KafkaPartitionSnapshot(int PartitionId, bool HasLeader);

internal sealed record KafkaTopicSnapshot(
    string Name,
    bool Unknown,
    bool AuthorizationFailed,
    string? OtherError,
    IReadOnlyList<KafkaPartitionSnapshot> Partitions);

internal sealed record KafkaGroupMemberSnapshot(string MemberId);

internal sealed record KafkaGroupSnapshot(
    bool NotFound,
    string? Failure,
    IReadOnlyList<KafkaGroupMemberSnapshot> Members);

/// <summary>
/// Narrow read of cluster metadata so validation can be tested without a live admin client.
/// </summary>
internal interface IKafkaMetadataClient
{
    Task ProbeConnectionAsync(TimeSpan timeout, CancellationToken cancellationToken);

    Task<KafkaTopicSnapshot> ReadTopicAsync(string topic, TimeSpan timeout, CancellationToken cancellationToken);

    Task<KafkaGroupSnapshot> ReadGroupAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken);
}
