namespace OmniBroker.Kafka.ServiceSetup;

internal static class KafkaRpcReplyStrings
{
    internal static string Prefix(KafkaSettings settings, string brokerName)
        => $"{settings.GroupId ?? brokerName}.rpc-reply";

    internal static string ForPartition(KafkaSettings settings, string brokerName, int partitionId)
        => $"{Prefix(settings, brokerName)}-{partitionId}";

    internal static bool IsFree(KafkaGroupSnapshot group)
        => group.Failure is null && (group.NotFound || group.Members.Count == 0);

    internal static string MissingTopicMessage()
        => "Kafka RPC callers require RpcReplyTopic.";

    internal static string MaximumExceededMessage(string topic, string prefix, int partitionCount)
        => $"Kafka RPC reply topic '{topic}' has no free partition in groups '{prefix}-*'. All {partitionCount} partition(s) are held, so the maximum is exceeded.";

    internal static string NoLeaderMessage(string topic)
        => $"Kafka RPC reply topic '{topic}' has no partition with a leader that this instance can take.";

    internal static string GroupFailureMessage(string groupId, string topic, string failure)
        => $"Kafka consumer group '{groupId}' for RPC reply topic '{topic}' cannot be read: {failure}.";

    internal static string ExclusiveClaimFailedMessage(string topic, int partitionId, string groupId)
        => $"Kafka RPC reply topic '{topic}' partition {partitionId} in group '{groupId}' could not be claimed exclusively.";

    internal static string JoinFailedMessage(string topic, string groupId)
        => $"Kafka RPC reply topic '{topic}' could not join group '{groupId}'.";
}
