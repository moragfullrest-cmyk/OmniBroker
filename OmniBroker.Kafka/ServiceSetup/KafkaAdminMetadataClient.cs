using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace OmniBroker.Kafka.ServiceSetup;

internal sealed class KafkaAdminMetadataClient : IKafkaMetadataClient, IDisposable
{
    internal static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private readonly IAdminClient _admin;

    public KafkaAdminMetadataClient(KafkaSettings settings)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = settings.Hosts
        };
        if (settings.ClientId is not null)
            config.ClientId = settings.ClientId;
        KafkaClientSecurity.Apply(settings, config);
        _admin = new AdminClientBuilder(config).Build();
    }

    public Task ProbeConnectionAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _admin.GetMetadata(timeout);
        return Task.CompletedTask;
    }

    public Task<KafkaTopicSnapshot> ReadTopicAsync(string topic, TimeSpan timeout, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Metadata metadata = _admin.GetMetadata(topic, timeout);
        TopicMetadata? topicMetadata = metadata.Topics.FirstOrDefault(item => item.Topic == topic);
        if (topicMetadata is null)
        {
            return Task.FromResult(new KafkaTopicSnapshot(
                topic,
                Unknown: true,
                AuthorizationFailed: false,
                OtherError: null,
                Partitions: []));
        }

        return Task.FromResult(ToSnapshot(topic, topicMetadata));
    }

    public async Task<KafkaGroupSnapshot> ReadGroupAsync(
        string groupId,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        DescribeConsumerGroupsResult result = await _admin.DescribeConsumerGroupsAsync(
            [groupId],
            new DescribeConsumerGroupsOptions { RequestTimeout = timeout });

        ConsumerGroupDescription? description = result.ConsumerGroupDescriptions.FirstOrDefault();
        if (description is null)
            return new KafkaGroupSnapshot(NotFound: false, Failure: "consumer group description was empty", Members: []);

        if (description.Error?.Code == ErrorCode.GroupIdNotFound)
            return new KafkaGroupSnapshot(NotFound: true, Failure: null, Members: []);

        if (description.Error is { IsError: true } error)
            return new KafkaGroupSnapshot(NotFound: false, Failure: error.Reason, Members: []);

        var members = new List<KafkaGroupMemberSnapshot>();
        if (description.Members is not null)
        {
            foreach (MemberDescription member in description.Members)
            {
                string? consumerId = member.ConsumerId;
                members.Add(new KafkaGroupMemberSnapshot(consumerId ?? string.Empty));
            }
        }

        return new KafkaGroupSnapshot(NotFound: false, Failure: null, Members: members);
    }

    public void Dispose() => _admin.Dispose();

    private static KafkaTopicSnapshot ToSnapshot(string topic, TopicMetadata topicMetadata)
    {
        Error error = topicMetadata.Error;
        bool unknown = error.Code == ErrorCode.UnknownTopicOrPart;
        bool authorizationFailed = error.Code == ErrorCode.TopicAuthorizationFailed;
        string? otherError = error.IsError && !unknown && !authorizationFailed ? error.Reason : null;
        var partitions = topicMetadata.Partitions
            .Select(partition => new KafkaPartitionSnapshot(
                partition.PartitionId,
                partition.Leader >= 0 && !partition.Error.IsError))
            .ToList();
        return new KafkaTopicSnapshot(topic, unknown, authorizationFailed, otherError, partitions);
    }
}
