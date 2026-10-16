using OmniBroker;
using OmniBroker.Infrastructure;

namespace OmniBroker.Kafka.ServiceSetup;

internal interface IKafkaReplyGroupSession : IAsyncDisposable
{
    string MemberId { get; }

    Task JoinAsync(TimeSpan timeout, CancellationToken cancellationToken);

    void Poll(TimeSpan timeout);
}

internal interface IKafkaReplyGroupSessionFactory
{
    IKafkaReplyGroupSession Open(string groupId);
}

internal sealed class KafkaRpcReplyHold(int partitionId, string groupId, IKafkaReplyGroupSession session)
{
    public int PartitionId { get; } = partitionId;

    public string GroupId { get; } = groupId;

    public IKafkaReplyGroupSession Session { get; } = session;
}

internal static class KafkaRpcReplyClaim
{
    internal static async Task<KafkaRpcReplyHold> ClaimAsync(
        string brokerId,
        KafkaSettings settings,
        BrokerOptionsBuilder builder,
        IKafkaMetadataClient metadata,
        IKafkaReplyGroupSessionFactory sessions,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(metadata);
        ArgumentNullException.ThrowIfNull(sessions);
        if (builder.RpcCallers.Count == 0)
            throw new InvalidOperationException("RPC reply partition is claimed only when RPC callers are configured.");

        if (string.IsNullOrWhiteSpace(settings.RpcReplyTopic))
            throw new BrokerStateException(brokerId, KafkaRpcReplyStrings.MissingTopicMessage());

        string topic = settings.RpcReplyTopic;
        KafkaTopicSnapshot reply = await metadata.ReadTopicAsync(topic, timeout, cancellationToken);
        if (reply.Unknown || reply.AuthorizationFailed || reply.OtherError is not null)
        {
            throw new BrokerStateException(
                brokerId,
                $"Kafka RPC reply topic '{topic}' is not usable.");
        }

        bool anyUnoccupied = false;
        bool anyFreeWithLeader = false;
        foreach (KafkaPartitionSnapshot partition in reply.Partitions.OrderBy(item => item.PartitionId))
        {
            cancellationToken.ThrowIfCancellationRequested();
            string groupId = KafkaRpcReplyStrings.ForPartition(settings, builder.SetupName, partition.PartitionId);
            KafkaGroupSnapshot group = await metadata.ReadGroupAsync(groupId, timeout, cancellationToken);
            if (group.Failure is not null)
            {
                throw new BrokerStateException(
                    brokerId,
                    KafkaRpcReplyStrings.GroupFailureMessage(groupId, topic, group.Failure));
            }

            if (!KafkaRpcReplyStrings.IsFree(group))
                continue;

            anyUnoccupied = true;
            if (!partition.HasLeader)
                continue;

            anyFreeWithLeader = true;
            IKafkaReplyGroupSession session = sessions.Open(groupId);
            bool claimed = false;
            try
            {
                try
                {
                    await session.JoinAsync(timeout, cancellationToken);
                }
                catch (Exception ex) when (ex is not BrokerStateException and not OperationCanceledException)
                {
                    throw new BrokerStateException(
                        brokerId,
                        KafkaRpcReplyStrings.JoinFailedMessage(topic, groupId),
                        ex);
                }

                if (await ConfirmExclusiveAsync(brokerId, topic, partition.PartitionId, groupId, session, metadata, timeout, cancellationToken))
                {
                    claimed = true;
                    return new KafkaRpcReplyHold(partition.PartitionId, groupId, session);
                }
            }
            finally
            {
                if (!claimed)
                    await session.DisposeAsync();
            }
        }

        if (!anyUnoccupied || anyFreeWithLeader)
        {
            throw new BrokerStateException(
                brokerId,
                KafkaRpcReplyStrings.MaximumExceededMessage(topic, KafkaRpcReplyStrings.Prefix(settings, builder.SetupName), reply.Partitions.Count));
        }

        throw new BrokerStateException(brokerId, KafkaRpcReplyStrings.NoLeaderMessage(topic));
    }

    private static async Task<bool> ConfirmExclusiveAsync(
        string brokerId,
        string topic,
        int partitionId,
        string groupId,
        IKafkaReplyGroupSession session,
        IKafkaMetadataClient metadata,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            KafkaGroupSnapshot group = await metadata.ReadGroupAsync(groupId, timeout, cancellationToken);
            if (group.Failure is not null)
            {
                throw new BrokerStateException(
                    brokerId,
                    KafkaRpcReplyStrings.GroupFailureMessage(groupId, topic, group.Failure));
            }

            if (group.Members.Any(member => member.MemberId == session.MemberId))
            {
                List<KafkaGroupMemberSnapshot> others = group.Members
                    .Where(member => member.MemberId != session.MemberId)
                    .ToList();
                if (others.Count == 0)
                    return true;

                if (others.Any(member => string.IsNullOrEmpty(member.MemberId) || string.CompareOrdinal(member.MemberId, session.MemberId) < 0))
                    return false;
            }

            if (DateTime.UtcNow >= deadline)
            {
                throw new BrokerStateException(
                    brokerId,
                    KafkaRpcReplyStrings.ExclusiveClaimFailedMessage(topic, partitionId, groupId));
            }

            session.Poll(TimeSpan.FromMilliseconds(100));
        }
    }
}
