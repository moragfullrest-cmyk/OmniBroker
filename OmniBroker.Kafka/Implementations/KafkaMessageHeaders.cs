namespace OmniBroker.Kafka.Implementations;

internal static class KafkaMessageHeaders
{
    public const string CorrelationId = "correlation-id";
    public const string ReplyTopic = "reply-topic";
    public const string ReplyPartition = "reply-partition";
    public const string SourceTopic = "source-topic";
    public const string Error = "error";
}
