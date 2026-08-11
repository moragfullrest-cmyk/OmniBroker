using System.Collections.Concurrent;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ;
using OmniBroker.RabbitMQ.Implementations;
using OmniBroker.RabbitMQ.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using RabbitMQ.Client;
using Shouldly;

namespace OmniBroker.Tests.Unit.RabbitMq;

public sealed class RabbitMQRpcTests
{
    private static RabbitMQSettings Settings(TimeSpan? timeout = null) => new()
    {
        HostName = "localhost",
        UserName = "guest",
        Password = "guest",
        RpcTimeout = timeout ?? TimeSpan.FromSeconds(5)
    };

    private static (RabbitMQBasicRpcCaller<TestMessage, TestReplyMessage> Caller,
        Mock<IProducer<TestMessage>> Producer,
        ConcurrentDictionary<string, TaskCompletionSource<IMessage>> Pending,
        BrokerOptionsBuilder Builder) CreateCaller(TimeSpan? timeout = null)
    {
        var brokerId = new BrokerId();
        var builder = new BrokerOptionsBuilder { BrokerId = brokerId, SetupName = "rpc-test" };
        builder.UseRabbitMq(Settings(timeout));
        ((RabbitMQExtension)builder.Extension!).ReplyQueueName = "reply-q";

        var producer = new Mock<IProducer<TestMessage>>();
        producer.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var pending = new ConcurrentDictionary<string, TaskCompletionSource<IMessage>>();
        var services = new ServiceCollection();
        services.AddSingleton(builder);
        services.AddKeyedSingleton(brokerId, builder);
        services.AddKeyedSingleton<IProducer<TestMessage>>(brokerId, producer.Object);
        services.AddKeyedSingleton(brokerId, pending);
        services.AddKeyedSingleton<INameResolver>(brokerId, new RabbitMQNameResolver(builder.SetupName));
        services.AddKeyedSingleton(brokerId, Settings(timeout));

        var caller = new RabbitMQBasicRpcCaller<TestMessage, TestReplyMessage>(services.BuildServiceProvider(), brokerId);
        return (caller, producer, pending, builder);
    }

    [Fact]
    public async Task Call_happy_path_returns_reply()
    {
        (RabbitMQBasicRpcCaller<TestMessage, TestReplyMessage> caller, Mock<IProducer<TestMessage>> producer, ConcurrentDictionary<string, TaskCompletionSource<IMessage>> pending, _) =
            CreateCaller();

        producer.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .Returns<TestMessage, PublishOptions?, CancellationToken>((_, options, _) =>
            {
                string correlationId = options!.CorrelationId!;
                _ = Task.Run(() =>
                {
                    if (pending.TryGetValue(correlationId, out TaskCompletionSource<IMessage>? tcs))
                    {
                        tcs.TrySetResult(new TestReplyMessage { CorrelationId = correlationId, Body = [9] });
                    }
                });
                return Task.FromResult(true);
            });

        TestReplyMessage reply = await caller.Call(new TestMessage { Body = [1] });

        reply.Body.ShouldBe([9]);
        pending.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public async Task Call_publish_false_throws()
    {
        (RabbitMQBasicRpcCaller<TestMessage, TestReplyMessage> caller, Mock<IProducer<TestMessage>> producer, _, _) = CreateCaller();
        producer.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        await Should.ThrowAsync<InvalidOperationException>(() => caller.Call(new TestMessage()));
    }

    [Fact]
    public async Task Call_timeout_throws_TimeoutException()
    {
        (RabbitMQBasicRpcCaller<TestMessage, TestReplyMessage> caller, _, _, _) = CreateCaller(TimeSpan.FromMilliseconds(50));

        await Should.ThrowAsync<TimeoutException>(() => caller.Call(new TestMessage()));
    }

    [Fact]
    public async Task Call_cancellation_throws()
    {
        (RabbitMQBasicRpcCaller<TestMessage, TestReplyMessage> caller, _, _, _) = CreateCaller(TimeSpan.FromMinutes(1));
        using var cts = new CancellationTokenSource(50);

        await Should.ThrowAsync<OperationCanceledException>(() => caller.Call(new TestMessage(), cts.Token));
    }

    [Fact]
    public async Task Call_finally_removes_pending_correlation()
    {
        (RabbitMQBasicRpcCaller<TestMessage, TestReplyMessage> caller, Mock<IProducer<TestMessage>> producer, ConcurrentDictionary<string, TaskCompletionSource<IMessage>> pending, _) =
            CreateCaller(TimeSpan.FromMilliseconds(30));
        producer.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        await Should.ThrowAsync<TimeoutException>(() => caller.Call(new TestMessage()));

        pending.IsEmpty.ShouldBeTrue();
    }

    [Fact]
    public async Task CorrelationDelegate_empty_correlation_returns_false()
    {
        MethodInfo method = typeof(RabbitMQExtension)
            .GetMethod("CreateCorrelationDelegate", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(TestReplyMessage));
        var func = (Func<ConcurrentDictionary<string, TaskCompletionSource<IMessage>>, TestReplyMessage, Task<bool>>)method.Invoke(null, null)!;
        var pending = new ConcurrentDictionary<string, TaskCompletionSource<IMessage>>();

        bool result = await func(pending, new TestReplyMessage { CorrelationId = "" });

        result.ShouldBeFalse();
    }

    [Fact]
    public async Task ReplyDelegate_null_reply_throws()
    {
        MethodInfo method = typeof(RabbitMQExtension)
            .GetMethod("CreateReplyDelegate", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(TestReplyMessage));
        var del = (Func<IServiceProvider, IMessage, MessageContext, Task<bool>>)method.Invoke(
            null,
            [(Delegate)(Func<TestMessage, Task<TestReplyMessage?>>)(_ => Task.FromResult<TestReplyMessage?>(null))])!;

        await Should.ThrowAsync<InvalidOperationException>(() =>
            del(new ServiceCollection().BuildServiceProvider(), new TestMessage(), new MessageContext(new BrokerId(), null)));
    }

    [Fact]
    public async Task ReplyDelegate_missing_ReplyTo_throws()
    {
        MethodInfo method = typeof(RabbitMQExtension)
            .GetMethod("CreateReplyDelegate", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(typeof(TestReplyMessage));
        var del = (Func<IServiceProvider, IMessage, MessageContext, Task<bool>>)method.Invoke(
            null,
            [(Delegate)(Func<TestMessage, Task<TestReplyMessage>>)(_ => Task.FromResult(new TestReplyMessage()))])!;

        await Should.ThrowAsync<InvalidOperationException>(() =>
            del(new ServiceCollection().BuildServiceProvider(), new TestMessage(), new MessageContext(new BrokerId(), null)));
    }

    [Fact]
    public async Task Recover_cancels_pending_rpc()
    {
        var brokerId = new BrokerId();
        var builder = new BrokerOptionsBuilder { BrokerId = brokerId, SetupName = "rpc-test" };
        builder.UseRabbitMq(Settings());
        var extension = (RabbitMQExtension)builder.Extension!;
        extension.ReplyQueueName = "reply-q";

        var pending = new ConcurrentDictionary<string, TaskCompletionSource<IMessage>>();
        var tcs = new TaskCompletionSource<IMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        pending["corr"] = tcs;

        var runtime = new RabbitMqRuntime
        {
            Connection = Mock.Of<IConnection>(c => c.IsOpen == true),
            ConsumerChannel = Mock.Of<IChannel>(c => c.IsOpen == true)
        };

        var services = new ServiceCollection();
        services.AddSingleton(builder);
        services.AddKeyedSingleton(brokerId, builder);
        services.AddKeyedSingleton(brokerId, pending);
        services.AddKeyedSingleton(brokerId, runtime);
        services.AddKeyedSingleton<INameResolver>(brokerId, new RabbitMQNameResolver(builder.SetupName));
        ServiceProvider provider = services.BuildServiceProvider();

        // Connection and consumer open → RecoverAsync returns early without cancelling.
        // Force recover path by marking connection closed.
        runtime.Connection = Mock.Of<IConnection>(c => c.IsOpen == false);
        runtime.ConsumerChannel = Mock.Of<IChannel>(c => c.IsOpen == false);

        // RecoverAsync will try StartInfrastructure (real connection) — instead invoke CancelPendingRpc via reflection.
        MethodInfo cancel = typeof(RabbitMQExtension).GetMethod("CancelPendingRpc", BindingFlags.Instance | BindingFlags.NonPublic)!;
        cancel.Invoke(extension, [provider]);

        pending.IsEmpty.ShouldBeTrue();
        Exception? ex = await Should.ThrowAsync<InvalidOperationException>(async () => await tcs.Task);
        ex!.Message.ShouldContain("recovered");
    }
}
