using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using OmniBroker;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.Implementations;
using OmniBroker.Kafka.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Kafka;

public sealed class KafkaRpcTests
{
    private static KafkaSettings Settings(TimeSpan? timeout = null) => new()
    {
        Hosts = "localhost:9092",
        RpcReplyTopic = "rpc-replies",
        RpcTimeout = timeout ?? TimeSpan.FromSeconds(5)
    };

    private static (KafkaBasicRpcCaller<TestMessage, TestReplyMessage> Caller,
        Mock<IProducer<TestMessage>> Producer,
        ConcurrentDictionary<string, KafkaRpcWait> Pending,
        KafkaRpcReplyLease Lease) CreateCaller(TimeSpan? timeout = null, bool ready = true, int partition = 2)
    {
        var brokerId = Guid.NewGuid().ToString();
        var lease = new KafkaRpcReplyLease();
        lease.Claim(partition, "setup.rpc-reply-2");
        if (ready)
            lease.MarkReady();

        var producer = new Mock<IProducer<TestMessage>>();
        producer.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var pending = new ConcurrentDictionary<string, KafkaRpcWait>();
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IProducer<TestMessage>>(brokerId, producer.Object);
        services.AddKeyedSingleton(brokerId, pending);
        services.AddKeyedSingleton<INameResolver>(brokerId, new KafkaNameResolver());
        services.AddKeyedSingleton(brokerId, Settings(timeout));
        services.AddKeyedSingleton(brokerId, lease);

        var caller = new KafkaBasicRpcCaller<TestMessage, TestReplyMessage>(services.BuildServiceProvider(), brokerId);
        return (caller, producer, pending, lease);
    }

    [Fact]
    public async Task Call_happy_path_returns_reply()
    {
        (KafkaBasicRpcCaller<TestMessage, TestReplyMessage> caller, Mock<IProducer<TestMessage>> producer, ConcurrentDictionary<string, KafkaRpcWait> pending, _) =
            CreateCaller();

        string? correlationId = null;
        producer.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .Returns<TestMessage, PublishOptions?, CancellationToken>((_, options, _) =>
            {
                correlationId = options!.CorrelationId;
                KafkaRpcCorrelation.TryComplete(pending, correlationId, [9]);
                return Task.FromResult(true);
            });

        TestReplyMessage reply = await caller.Call(new TestMessage { Body = [1] });

        reply.Body.ShouldBe([9]);
        reply.CorrelationId.ShouldBe(correlationId);
        string.IsNullOrWhiteSpace(correlationId).ShouldBeFalse();
        pending.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public async Task Call_publishes_correlation_and_reply_partition()
    {
        (KafkaBasicRpcCaller<TestMessage, TestReplyMessage> caller, Mock<IProducer<TestMessage>> producer, ConcurrentDictionary<string, KafkaRpcWait> pending, _) =
            CreateCaller(partition: 4);
        PublishOptions? captured = null;
        TestMessage? published = null;
        producer.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .Callback<TestMessage, PublishOptions?, CancellationToken>((message, options, _) =>
            {
                published = message;
                captured = options;
                KafkaRpcCorrelation.TryComplete(pending, options!.CorrelationId, [1]);
            })
            .ReturnsAsync(true);

        await caller.Call(new TestMessage { Body = [1] });

        published!.Tag.ShouldBe(nameof(TestMessage));
        captured.ShouldNotBeNull();
        string.IsNullOrWhiteSpace(captured.CorrelationId).ShouldBeFalse();
        captured.ReplyTo.ShouldBe("rpc-replies");
        captured.Destination.ShouldBe(nameof(TestMessage));
        KafkaPublishOptions kafkaOptions = captured.ShouldBeOfType<KafkaPublishOptions>();
        kafkaOptions.ReplyPartition.ShouldBe(4);
    }

    [Fact]
    public async Task Call_publish_false_throws()
    {
        (KafkaBasicRpcCaller<TestMessage, TestReplyMessage> caller, Mock<IProducer<TestMessage>> producer, ConcurrentDictionary<string, KafkaRpcWait> pending, _) =
            CreateCaller();
        producer.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        await Should.ThrowAsync<InvalidOperationException>(() => caller.Call(new TestMessage()));
        pending.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public async Task Call_timeout_throws_TimeoutException()
    {
        (KafkaBasicRpcCaller<TestMessage, TestReplyMessage> caller, _, ConcurrentDictionary<string, KafkaRpcWait> pending, _) =
            CreateCaller(TimeSpan.FromMilliseconds(50));

        await Should.ThrowAsync<TimeoutException>(() => caller.Call(new TestMessage()));
        pending.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public async Task Call_cancellation_throws()
    {
        (KafkaBasicRpcCaller<TestMessage, TestReplyMessage> caller, Mock<IProducer<TestMessage>> producer, ConcurrentDictionary<string, KafkaRpcWait> pending, _) =
            CreateCaller(TimeSpan.FromMinutes(1));

        KafkaRpcWait? captured = null;
        producer.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .Callback<TestMessage, PublishOptions?, CancellationToken>((_, options, _) =>
            {
                captured = pending[options!.CorrelationId!];
            })
            .ReturnsAsync(true);

        using var cts = new CancellationTokenSource(50);

        await Should.ThrowAsync<OperationCanceledException>(() => caller.Call(new TestMessage(), cts.Token));

        pending.IsEmpty.ShouldBeTrue();
        captured.ShouldNotBeNull();
        captured!.Completion.Task.IsCanceled.ShouldBeTrue();
    }

    [Fact]
    public async Task Call_before_partition_is_ready_waits()
    {
        (KafkaBasicRpcCaller<TestMessage, TestReplyMessage> caller, Mock<IProducer<TestMessage>> producer, ConcurrentDictionary<string, KafkaRpcWait> pending, _) =
            CreateCaller(ready: false);

        using var cts = new CancellationTokenSource(50);

        await Should.ThrowAsync<OperationCanceledException>(() => caller.Call(new TestMessage(), cts.Token));

        pending.IsEmpty.ShouldBeTrue();
        producer.Verify(
            p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Call_faulted_lease_throws()
    {
        (KafkaBasicRpcCaller<TestMessage, TestReplyMessage> caller, _, _, KafkaRpcReplyLease lease) = CreateCaller(ready: false);
        lease.Fail(new InvalidOperationException("claim failed"));

        InvalidOperationException ex = await Should.ThrowAsync<InvalidOperationException>(() => caller.Call(new TestMessage()));
        ex.Message.ShouldContain("claim failed");
    }

    [Fact]
    public void TryComplete_empty_correlation_does_not_finish_a_task()
    {
        var pending = new ConcurrentDictionary<string, KafkaRpcWait>();
        var wait = new KafkaRpcWait(
            new TaskCompletionSource<IMessage>(TaskCreationOptions.RunContinuationsAsynchronously),
            HandlerWrapper.BuildCreateMessage(typeof(TestReplyMessage)));
        pending["corr"] = wait;

        bool completed = KafkaRpcCorrelation.TryComplete(pending, "", [1]);

        completed.ShouldBeFalse();
        pending.ContainsKey("corr").ShouldBeTrue();
        wait.Completion.Task.IsCompleted.ShouldBeFalse();
    }

    [Fact]
    public void TryComplete_unknown_correlation_returns_false()
    {
        var pending = new ConcurrentDictionary<string, KafkaRpcWait>();

        bool completed = KafkaRpcCorrelation.TryComplete(pending, "missing", [1]);

        completed.ShouldBeFalse();
    }

    [Fact]
    public void TryComplete_known_correlation_sets_body_and_removes_wait()
    {
        var pending = new ConcurrentDictionary<string, KafkaRpcWait>();
        var wait = new KafkaRpcWait(
            new TaskCompletionSource<IMessage>(TaskCreationOptions.RunContinuationsAsynchronously),
            HandlerWrapper.BuildCreateMessage(typeof(TestReplyMessage)));
        pending["corr"] = wait;

        bool completed = KafkaRpcCorrelation.TryComplete(pending, "corr", [9]);

        completed.ShouldBeTrue();
        pending.IsEmpty.ShouldBeTrue();
        wait.Completion.Task.IsCompletedSuccessfully.ShouldBeTrue();
        TestReplyMessage reply = wait.Completion.Task.Result.ShouldBeOfType<TestReplyMessage>();
        reply.Body.ShouldBe([9]);
        reply.CorrelationId.ShouldBe("corr");
    }

    [Fact]
    public void CancelPending_cancels_waiting_tasks()
    {
        var pending = new ConcurrentDictionary<string, KafkaRpcWait>();
        var wait = new KafkaRpcWait(
            new TaskCompletionSource<IMessage>(TaskCreationOptions.RunContinuationsAsynchronously),
            HandlerWrapper.BuildCreateMessage(typeof(TestReplyMessage)));
        pending["corr"] = wait;

        KafkaRpcCorrelation.CancelPending(pending);

        pending.IsEmpty.ShouldBeTrue();
        wait.Completion.Task.IsCanceled.ShouldBeTrue();
    }

    [Fact]
    public async Task ReplyDelegate_null_reply_throws()
    {
        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> del = ReplyDelegate(
            (Func<TestMessage, Task<TestReplyMessage?>>)(_ => Task.FromResult<TestReplyMessage?>(null)));

        await Should.ThrowAsync<InvalidOperationException>(() =>
            del(new ServiceCollection().BuildServiceProvider(), new TestMessage(), new MessageContext(Guid.NewGuid().ToString(), null)));
    }

    [Fact]
    public async Task ReplyDelegate_missing_reply_address_throws()
    {
        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> del = ReplyDelegate(
            (Func<TestMessage, Task<TestReplyMessage>>)(_ => Task.FromResult(new TestReplyMessage())));

        await Should.ThrowAsync<InvalidOperationException>(() =>
            del(new ServiceCollection().BuildServiceProvider(), new TestMessage { CorrelationId = "corr" }, new MessageContext(Guid.NewGuid().ToString(), null)));
    }

    [Fact]
    public async Task ReplyDelegate_publishes_same_correlation_to_reply_partition()
    {
        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> del = ReplyDelegate(
            (Func<TestMessage, Task<TestReplyMessage>>)(_ => Task.FromResult(new TestReplyMessage { Body = [1] })));

        var brokerId = Guid.NewGuid().ToString();
        PublishOptions? captured = null;
        var producer = new Mock<IProducer<TestReplyMessage>>();
        producer.Setup(p => p.Publish(It.IsAny<TestReplyMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .Callback<TestReplyMessage, PublishOptions?, CancellationToken>((_, options, _) => captured = options)
            .ReturnsAsync(true);

        var services = new ServiceCollection();
        services.AddKeyedSingleton<IProducer<TestReplyMessage>>(brokerId, producer.Object);
        using var cts = new CancellationTokenSource();

        HandleResult result = await del(
            services.BuildServiceProvider(),
            new TestMessage { CorrelationId = "corr" },
            new MessageContext(brokerId, new KafkaReplyInfo { ReplyTopic = "rpc-replies", ReplyPartition = 3 }, cts.Token));

        result.ShouldBe(HandleResult.Ack);
        captured.ShouldNotBeNull();
        captured!.CorrelationId.ShouldBe("corr");
        captured.Destination.ShouldBe("rpc-replies");
        captured.ReplyTo.ShouldBeNull();
        KafkaPublishOptions kafkaOptions = captured.ShouldBeOfType<KafkaPublishOptions>();
        kafkaOptions.ReplyPartition.ShouldBe(3);
        producer.Verify(p => p.Publish(It.IsAny<TestReplyMessage>(), It.IsAny<PublishOptions?>(), cts.Token), Times.Once);
    }

    [Fact]
    public async Task ReplyDelegate_publish_false_returns_retry()
    {
        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> del = ReplyDelegate(
            (Func<TestMessage, Task<TestReplyMessage>>)(_ => Task.FromResult(new TestReplyMessage())));

        var brokerId = Guid.NewGuid().ToString();
        var producer = new Mock<IProducer<TestReplyMessage>>();
        producer.Setup(p => p.Publish(It.IsAny<TestReplyMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        var services = new ServiceCollection();
        services.AddKeyedSingleton<IProducer<TestReplyMessage>>(brokerId, producer.Object);

        HandleResult result = await del(
            services.BuildServiceProvider(),
            new TestMessage { CorrelationId = "corr" },
            new MessageContext(brokerId, new KafkaReplyInfo { ReplyTopic = "rpc-replies", ReplyPartition = 1 }));

        result.ShouldBe(HandleResult.Retry);
    }

    private static Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> ReplyDelegate(Delegate action)
    {
        MethodInfo method = typeof(KafkaExtension)
            .GetMethod("CreateReplyDelegate", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(TestReplyMessage));
        return (Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>>)method.Invoke(null, [action])!;
    }
}
