using OmniBroker.Infrastructure;

namespace OmniBroker.Kafka.Implementations;

/// <summary>
/// Kafka publish options.
/// ReplyPartition is the caller's partition of the reply topic.
/// When ReplyTo is set, the number is written to the reply-partition header and this record keeps key-based partitioning.
/// When ReplyTo is empty, this record is produced to that partition.
/// </summary>
internal sealed record KafkaPublishOptions(
    string? CorrelationId,
    string? ReplyTo,
    string? Destination,
    int? ReplyPartition = null) : PublishOptions(CorrelationId, ReplyTo, Destination);
