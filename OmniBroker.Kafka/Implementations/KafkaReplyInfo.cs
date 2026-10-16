using OmniBroker.Interfaces;

namespace OmniBroker.Kafka.Implementations;

internal sealed class KafkaReplyInfo : IReplyInfo
{
    public string? ReplyTopic { get; init; }

    public int? ReplyPartition { get; init; }
}
