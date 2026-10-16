using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using OmniBroker;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.Implementations;
using OmniBroker.Kafka.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Kafka;

public sealed class KafkaRemoteStateTests
{
    [Fact]
    public async Task ValidateRemoteBrokerState_unreachable_cluster_throws()
    {
        var metadata = new Mock<IKafkaMetadataClient>();
        metadata.Setup(m => m.ProbeConnectionAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new KafkaException(new Error(ErrorCode.Local_Transport, "down")));
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(new KafkaSettings
        {
            Hosts = "localhost:9092",
            SaslPassword = "sasl-secret"
        });
        builder.AddProducerFor<TestMessage>();

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(
            () => extension.ValidateRemoteBrokerState(provider, builder, metadata.Object));

        ex.BrokerId.ShouldBe("kafka");
        ex.Message.ShouldContain("unreachable");
        ex.Message.ShouldNotContain("sasl-secret");
        metadata.Verify(m => m.ReadTopicAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_lists_every_missing_topic()
    {
        var metadata = ReadyMetadata(_ => Unknown("ignored"));
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(new KafkaSettings
        {
            Hosts = "localhost:9092",
            DeadLetterTopic = "dead-letters",
            RpcReplyTopic = "rpc-replies"
        });
        builder.AddProducerFor<TestMessage>();
        builder.AddConsumerFor<AnotherTestMessage>((AnotherTestMessage _) => Task.FromResult(HandleResult.Ack));

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(
            () => extension.ValidateRemoteBrokerState(provider, builder, metadata.Object));

        ex.Message.ShouldContain(nameof(TestMessage));
        ex.Message.ShouldContain(nameof(AnotherTestMessage));
        ex.Message.ShouldContain("dead-letters");
        ex.Message.ShouldNotContain("rpc-replies");
        metadata.Verify(
            m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_reply_topic_without_callers_does_not_read_group()
    {
        var metadata = ReadyMetadata(_ => Unknown("ignored"));
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(new KafkaSettings
        {
            Hosts = "localhost:9092",
            GroupId = "workers",
            RpcReplyTopic = "rpc-replies"
        });

        await extension.ValidateRemoteBrokerState(provider, builder, metadata.Object);

        metadata.Verify(
            m => m.ReadTopicAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Never);
        metadata.Verify(
            m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_caller_free_reply_partition_passes()
    {
        var metadata = ReadyMetadata(topic => topic == "rpc-replies"
            ? Topic("rpc-replies", new KafkaPartitionSnapshot(0, HasLeader: true))
            : Unknown(topic));
        metadata.Setup(m => m.ReadGroupAsync("workers.rpc-reply-0", TimeSpan.FromSeconds(10), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KafkaGroupSnapshot(NotFound: true, Failure: null, Members: []));
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(new KafkaSettings
        {
            Hosts = "localhost:9092",
            GroupId = "workers",
            RpcReplyTopic = "rpc-replies"
        });
        builder.RpcCallers[typeof(TestMessage)] = typeof(TestReplyMessage);

        await extension.ValidateRemoteBrokerState(provider, builder, metadata.Object);

        metadata.Verify(
            m => m.ProbeConnectionAsync(TimeSpan.FromSeconds(10), It.IsAny<CancellationToken>()),
            Times.Once);
        metadata.Verify(
            m => m.ReadGroupAsync("workers.rpc-reply-0", TimeSpan.FromSeconds(10), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_callers_without_reply_topic_throws()
    {
        var metadata = new Mock<IKafkaMetadataClient>();
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(new KafkaSettings
        {
            Hosts = "localhost:9092"
        });
        builder.RpcCallers[typeof(TestMessage)] = typeof(TestReplyMessage);

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(
            () => extension.ValidateRemoteBrokerState(provider, builder, metadata.Object));

        ex.Message.ShouldContain("RpcReplyTopic");
        metadata.Verify(m => m.ProbeConnectionAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_receiver_only_does_not_read_reply_group()
    {
        var metadata = ReadyMetadata(_ => Topic("rpc-replies", new KafkaPartitionSnapshot(0, HasLeader: true)));
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(new KafkaSettings
        {
            Hosts = "localhost:9092",
            RpcReplyTopic = "rpc-replies"
        });
        builder.RpcReceivers[typeof(TestMessage)] = (Func<TestMessage, Task<TestReplyMessage>>)(_ => Task.FromResult(new TestReplyMessage()));

        await extension.ValidateRemoteBrokerState(provider, builder, metadata.Object);

        metadata.Verify(
            m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_reply_partitions_all_held_exceeds_maximum()
    {
        var metadata = ReadyMetadata(_ => Topic(
            "rpc-replies",
            new KafkaPartitionSnapshot(0, HasLeader: true),
            new KafkaPartitionSnapshot(1, HasLeader: true)));
        metadata.Setup(m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KafkaGroupSnapshot(
                NotFound: false,
                Failure: null,
                Members: [new KafkaGroupMemberSnapshot("member-a")]));
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(
            new KafkaSettings
            {
                Hosts = "localhost:9092",
                GroupId = "workers",
                RpcReplyTopic = "rpc-replies"
            },
            setupName: "setup");
        builder.RpcCallers[typeof(TestMessage)] = typeof(TestReplyMessage);

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(
            () => extension.ValidateRemoteBrokerState(provider, builder, metadata.Object));

        ex.Message.ShouldContain("maximum is exceeded");
        ex.Message.ShouldContain("rpc-replies");
        ex.Message.ShouldContain("workers.rpc-reply");
        metadata.Verify(
            m => m.ReadGroupAsync("workers.rpc-reply-0", TimeSpan.FromSeconds(10), It.IsAny<CancellationToken>()),
            Times.Once);
        metadata.Verify(
            m => m.ReadGroupAsync("workers.rpc-reply-1", TimeSpan.FromSeconds(10), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_reply_group_failure_throws()
    {
        var metadata = ReadyMetadata(_ => Topic("rpc-replies", new KafkaPartitionSnapshot(0, HasLeader: true)));
        metadata.Setup(m => m.ReadGroupAsync("setup.rpc-reply-0", It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KafkaGroupSnapshot(NotFound: false, Failure: "denied", Members: []));
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(new KafkaSettings
        {
            Hosts = "localhost:9092",
            RpcReplyTopic = "rpc-replies"
        });
        builder.RpcCallers[typeof(TestMessage)] = typeof(TestReplyMessage);

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(
            () => extension.ValidateRemoteBrokerState(provider, builder, metadata.Object));

        ex.Message.ShouldContain("cannot be read");
        ex.Message.ShouldContain("denied");
        ex.Message.ShouldContain("setup.rpc-reply-0");
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_without_reply_topic_does_not_read_group()
    {
        var metadata = ReadyMetadata(topic => Topic(topic, new KafkaPartitionSnapshot(0, HasLeader: true)));
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(new KafkaSettings
        {
            Hosts = "localhost:9092",
            SaslPassword = "sasl-secret"
        });
        builder.AddProducerFor<TestMessage>();

        await extension.ValidateRemoteBrokerState(provider, builder, metadata.Object);

        metadata.Verify(
            m => m.ReadGroupAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_free_partition_without_leader_throws()
    {
        var metadata = ReadyMetadata(_ => Topic("rpc-replies", new KafkaPartitionSnapshot(0, HasLeader: false)));
        metadata.Setup(m => m.ReadGroupAsync("setup.rpc-reply-0", It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new KafkaGroupSnapshot(NotFound: true, Failure: null, Members: []));
        (KafkaExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(new KafkaSettings
        {
            Hosts = "localhost:9092",
            RpcReplyTopic = "rpc-replies"
        });
        builder.RpcCallers[typeof(TestMessage)] = typeof(TestReplyMessage);

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(
            () => extension.ValidateRemoteBrokerState(provider, builder, metadata.Object));

        ex.Message.ShouldContain("no partition with a leader");
    }

    [Fact]
    public async Task SetupInfrastructure_registers_validation_before_consumer()
    {
        var builder = new BrokerOptionsBuilder { SetupName = "setup" };
        builder.BrokerId = "kafka";
        builder.UseKafka(new KafkaSettings { Hosts = "localhost:9092" });
        builder.AddConsumerFor<TestMessage>((TestMessage _) => Task.FromResult(HandleResult.Ack));
        var extension = (KafkaExtension)builder.Extension!;
        var services = new ServiceCollection();

        await extension.SetupInfrastructure(services, builder);
        await extension.SetupConsumers(services, builder);

        List<ServiceDescriptor> hosted = services.Where(descriptor => descriptor.ServiceType == typeof(IHostedService)).ToList();
        hosted.Count.ShouldBe(2);
        hosted[0].ImplementationFactory.ShouldNotBeNull();
        hosted[0].ImplementationFactory!(services.BuildServiceProvider()).ShouldBeOfType<KafkaBrokerHostedService>();
        hosted[1].ImplementationType.ShouldBe(typeof(KafkaConsumer));
    }

    [Fact]
    public async Task SetupInfrastructure_registers_reply_lease_before_consumer_when_callers_exist()
    {
        var builder = new BrokerOptionsBuilder { SetupName = "setup" };
        builder.BrokerId = "kafka";
        builder.UseKafka(new KafkaSettings { Hosts = "localhost:9092", RpcReplyTopic = "rpc-replies" });
        builder.RpcCallers[typeof(TestMessage)] = typeof(TestReplyMessage);
        builder.AddConsumerFor<AnotherTestMessage>((AnotherTestMessage _) => Task.FromResult(HandleResult.Ack));
        var extension = (KafkaExtension)builder.Extension!;
        var services = new ServiceCollection();

        await extension.SetupInfrastructure(services, builder);
        await extension.SetupConsumers(services, builder);

        using ServiceProvider provider = services.BuildServiceProvider();
        List<ServiceDescriptor> hosted = services.Where(descriptor => descriptor.ServiceType == typeof(IHostedService)).ToList();
        hosted.Count.ShouldBe(3);
        hosted[0].ImplementationFactory!(provider).ShouldBeOfType<KafkaBrokerHostedService>();
        hosted[1].ImplementationFactory!(provider).ShouldBeOfType<KafkaRpcReplyLeaseHostedService>();
        hosted[2].ImplementationType.ShouldBe(typeof(KafkaConsumer));
        services.Any(descriptor => descriptor.IsKeyedService
            && Equals(descriptor.ServiceKey, "kafka")
            && descriptor.ServiceType == typeof(KafkaRpcReplyLease)).ShouldBeTrue();
    }

    [Fact]
    public async Task SetupInfrastructure_receiver_only_does_not_register_reply_lease()
    {
        var builder = new BrokerOptionsBuilder { SetupName = "setup" };
        builder.BrokerId = "kafka";
        builder.UseKafka(new KafkaSettings { Hosts = "localhost:9092", RpcReplyTopic = "rpc-replies" });
        builder.RpcReceivers[typeof(TestMessage)] = (Func<TestMessage, Task<TestReplyMessage>>)(_ => Task.FromResult(new TestReplyMessage()));
        var extension = (KafkaExtension)builder.Extension!;
        var services = new ServiceCollection();

        await extension.SetupInfrastructure(services, builder);

        services.Count(descriptor => descriptor.ServiceType == typeof(IHostedService)).ShouldBe(1);
        services.Any(descriptor => descriptor.ServiceType == typeof(KafkaRpcReplyLease)).ShouldBeFalse();
    }

    private static Mock<IKafkaMetadataClient> ReadyMetadata(Func<string, KafkaTopicSnapshot> topic)
    {
        var metadata = new Mock<IKafkaMetadataClient>();
        metadata.Setup(m => m.ProbeConnectionAsync(It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        metadata.Setup(m => m.ReadTopicAsync(It.IsAny<string>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string name, TimeSpan _, CancellationToken _) => topic(name));
        return metadata;
    }

    private static KafkaTopicSnapshot Unknown(string name)
        => new(name, Unknown: true, AuthorizationFailed: false, OtherError: null, Partitions: []);

    private static KafkaTopicSnapshot Topic(string name, params KafkaPartitionSnapshot[] partitions)
        => new(name, Unknown: false, AuthorizationFailed: false, OtherError: null, Partitions: partitions);

    private static (KafkaExtension Extension, BrokerOptionsBuilder Builder, ServiceProvider Provider) Create(
        KafkaSettings settings,
        string setupName = "setup")
    {
        var builder = new BrokerOptionsBuilder { SetupName = setupName };
        builder.BrokerId = "kafka";
        builder.UseKafka(settings);
        var extension = (KafkaExtension)builder.Extension!;
        var services = new ServiceCollection();
        services.AddKeyedSingleton<INameResolver>(builder.BrokerId, new KafkaNameResolver());
        return (extension, builder, services.BuildServiceProvider());
    }
}
