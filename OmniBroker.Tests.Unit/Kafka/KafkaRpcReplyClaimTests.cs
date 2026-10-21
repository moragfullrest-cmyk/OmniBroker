using Moq;
using OmniBroker;
using OmniBroker.Infrastructure;
using OmniBroker.Kafka.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Kafka;

public sealed class KafkaRpcReplyClaimTests
{
    [Fact]
    public async Task ClaimAsync_claims_lowest_free_partition()
    {
        var metadata = TopicWith(
            new KafkaPartitionSnapshot(1, HasLeader: true),
            new KafkaPartitionSnapshot(0, HasLeader: true));
        var reads = new Dictionary<string, int>();
        metadata.Setup(m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string groupId, TimeSpan _, CancellationToken _) => NextRead(reads, groupId, freeThenOwned: true, ownerId: "member-a"));
        var sessions = new RecordingFactory(_ => new FakeSession("member-a"));
        BrokerOptionsBuilder builder = CallerBuilder();

        KafkaRpcReplyHold hold = await KafkaRpcReplyClaim.ClaimAsync(
            "kafka",
            Settings(),
            builder,
            metadata.Object,
            sessions,
            TimeSpan.FromSeconds(10),
            CancellationToken.None);

        hold.PartitionId.ShouldBe(0);
        hold.GroupId.ShouldBe("workers.rpc-reply-0");
        sessions.Opened.Count.ShouldBe(1);
        sessions.Opened[0].Disposed.ShouldBeFalse();
        metadata.Verify(m => m.ReadGroupAsync("workers.rpc-reply-0", It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        metadata.Verify(m => m.ReadGroupAsync("workers.rpc-reply-1", It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ClaimAsync_skips_occupied_partition()
    {
        var metadata = TopicWith(
            new KafkaPartitionSnapshot(0, HasLeader: true),
            new KafkaPartitionSnapshot(1, HasLeader: true));
        var reads = new Dictionary<string, int>();
        metadata.Setup(m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string groupId, TimeSpan _, CancellationToken _) =>
                groupId == "workers.rpc-reply-0"
                    ? Occupied("member-other")
                    : NextRead(reads, groupId, freeThenOwned: true, ownerId: "member-a"));
        var sessions = new RecordingFactory(_ => new FakeSession("member-a"));
        BrokerOptionsBuilder builder = CallerBuilder();

        KafkaRpcReplyHold hold = await KafkaRpcReplyClaim.ClaimAsync(
            "kafka",
            Settings(),
            builder,
            metadata.Object,
            sessions,
            TimeSpan.FromSeconds(10),
            CancellationToken.None);

        hold.PartitionId.ShouldBe(1);
        hold.GroupId.ShouldBe("workers.rpc-reply-1");
        sessions.Opened.Single().MemberId.ShouldBe("member-a");
    }

    [Fact]
    public async Task ClaimAsync_loses_race_to_smaller_member_and_takes_next()
    {
        var metadata = TopicWith(
            new KafkaPartitionSnapshot(0, HasLeader: true),
            new KafkaPartitionSnapshot(1, HasLeader: true));
        var reads = new Dictionary<string, int>();
        metadata.Setup(m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string groupId, TimeSpan _, CancellationToken _) =>
            {
                reads.TryGetValue(groupId, out int count);
                reads[groupId] = count + 1;
                if (count == 0)
                    return Free();

                return groupId == "workers.rpc-reply-0"
                    ? new KafkaGroupSnapshot(NotFound: false, Failure: null, Members:
                    [
                        new KafkaGroupMemberSnapshot("m-1"),
                        new KafkaGroupMemberSnapshot("m-2")
                    ])
                    : new KafkaGroupSnapshot(NotFound: false, Failure: null, Members: [new KafkaGroupMemberSnapshot("m-2")]);
            });
        var sessions = new RecordingFactory(_ => new FakeSession("m-2"));
        BrokerOptionsBuilder builder = CallerBuilder();

        KafkaRpcReplyHold hold = await KafkaRpcReplyClaim.ClaimAsync(
            "kafka",
            Settings(),
            builder,
            metadata.Object,
            sessions,
            TimeSpan.FromSeconds(10),
            CancellationToken.None);

        hold.PartitionId.ShouldBe(1);
        sessions.Opened.Count.ShouldBe(2);
        sessions.Opened[0].Disposed.ShouldBeTrue();
        sessions.Opened[1].Disposed.ShouldBeFalse();
    }

    [Fact]
    public async Task ClaimAsync_all_partitions_held_throws()
    {
        var metadata = TopicWith(
            new KafkaPartitionSnapshot(0, HasLeader: true),
            new KafkaPartitionSnapshot(1, HasLeader: true));
        metadata.Setup(m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Occupied("member-other"));
        var sessions = new RecordingFactory(_ => new FakeSession("member-a"));
        BrokerOptionsBuilder builder = CallerBuilder();

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(() => KafkaRpcReplyClaim.ClaimAsync(
            "kafka",
            Settings(),
            builder,
            metadata.Object,
            sessions,
            TimeSpan.FromSeconds(10),
            CancellationToken.None));

        ex.BrokerName.ShouldBe("kafka");
        ex.Message.ShouldContain("maximum is exceeded");
        ex.Message.ShouldContain("rpc-replies");
        sessions.Opened.ShouldBeEmpty();
    }

    [Fact]
    public async Task ClaimAsync_free_partition_without_leader_throws()
    {
        var metadata = TopicWith(new KafkaPartitionSnapshot(0, HasLeader: false));
        metadata.Setup(m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Free());
        var sessions = new RecordingFactory(_ => new FakeSession("member-a"));
        BrokerOptionsBuilder builder = CallerBuilder();

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(() => KafkaRpcReplyClaim.ClaimAsync(
            "kafka",
            Settings(),
            builder,
            metadata.Object,
            sessions,
            TimeSpan.FromSeconds(10),
            CancellationToken.None));

        ex.Message.ShouldContain("no partition with a leader");
        sessions.Opened.ShouldBeEmpty();
    }

    [Fact]
    public async Task ClaimAsync_without_reply_topic_throws()
    {
        var metadata = new Mock<IKafkaMetadataClient>();
        var sessions = new RecordingFactory(_ => new FakeSession("member-a"));
        BrokerOptionsBuilder builder = CallerBuilder();

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(() => KafkaRpcReplyClaim.ClaimAsync(
            "kafka",
            new KafkaSettings { Hosts = "localhost:9092", GroupId = "workers" },
            builder,
            metadata.Object,
            sessions,
            TimeSpan.FromSeconds(10),
            CancellationToken.None));

        ex.Message.ShouldContain("RpcReplyTopic");
        metadata.Verify(m => m.ReadTopicAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ClaimAsync_exclusive_wait_times_out_and_releases_session()
    {
        var metadata = TopicWith(new KafkaPartitionSnapshot(0, HasLeader: true));
        var reads = new Dictionary<string, int>();
        metadata.Setup(m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string groupId, TimeSpan _, CancellationToken _) =>
            {
                reads.TryGetValue(groupId, out int count);
                reads[groupId] = count + 1;
                if (count == 0)
                    return Free();

                return new KafkaGroupSnapshot(NotFound: false, Failure: null, Members:
                [
                    new KafkaGroupMemberSnapshot("m-2"),
                    new KafkaGroupMemberSnapshot("m-9")
                ]);
            });
        var sessions = new RecordingFactory(_ => new FakeSession("m-2"));
        BrokerOptionsBuilder builder = CallerBuilder();

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(() => KafkaRpcReplyClaim.ClaimAsync(
            "kafka",
            Settings(),
            builder,
            metadata.Object,
            sessions,
            TimeSpan.Zero,
            CancellationToken.None));

        ex.Message.ShouldContain("exclusively");
        ex.Message.ShouldContain("workers.rpc-reply-0");
        sessions.Opened.Single().Disposed.ShouldBeTrue();
    }

    private static KafkaGroupSnapshot NextRead(Dictionary<string, int> reads, string groupId, bool freeThenOwned, string ownerId)
    {
        reads.TryGetValue(groupId, out int count);
        reads[groupId] = count + 1;
        if (count == 0 && freeThenOwned)
            return Free();

        return new KafkaGroupSnapshot(NotFound: false, Failure: null, Members: [new KafkaGroupMemberSnapshot(ownerId)]);
    }

    private static KafkaGroupSnapshot Free()
        => new(NotFound: true, Failure: null, Members: []);

    private static KafkaGroupSnapshot Occupied(string memberId)
        => new(NotFound: false, Failure: null, Members: [new KafkaGroupMemberSnapshot(memberId)]);

    private static Mock<IKafkaMetadataClient> TopicWith(params KafkaPartitionSnapshot[] partitions)
    {
        var metadata = new Mock<IKafkaMetadataClient>();
        metadata.Setup(m => m.ReadTopicAsync("rpc-replies", It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KafkaTopicSnapshot(
                "rpc-replies",
                Unknown: false,
                AuthorizationFailed: false,
                OtherError: null,
                Partitions: partitions));
        return metadata;
    }

    private static KafkaSettings Settings()
        => new()
        {
            Hosts = "localhost:9092",
            GroupId = "workers",
            RpcReplyTopic = "rpc-replies"
        };

    private static BrokerOptionsBuilder CallerBuilder()
    {
        var builder = new BrokerOptionsBuilder { BrokerName = "kafka" };
        builder.RpcCallers[typeof(TestMessage)] = typeof(TestReplyMessage);
        return builder;
    }

    private sealed class RecordingFactory(Func<string, FakeSession> create) : IKafkaReplyGroupSessionFactory
    {
        public List<FakeSession> Opened { get; } = [];

        public IKafkaReplyGroupSession Open(string groupId)
        {
            FakeSession session = create(groupId);
            Opened.Add(session);
            return session;
        }
    }

    private sealed class FakeSession(string memberId) : IKafkaReplyGroupSession
    {
        public string MemberId { get; } = memberId;

        public bool Disposed { get; private set; }

        public Task JoinAsync(TimeSpan timeout, CancellationToken cancellationToken) => Task.CompletedTask;

        public void Poll(TimeSpan timeout)
        {
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
