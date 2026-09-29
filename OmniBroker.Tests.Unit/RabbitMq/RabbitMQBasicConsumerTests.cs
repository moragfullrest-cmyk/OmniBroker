using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.Implementations;
using OmniBroker.RabbitMQ.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shouldly;

namespace OmniBroker.Tests.Unit.RabbitMq;

public sealed class RabbitMQBasicConsumerTests
{
    private static RabbitMQSettings Settings(string? deadLetterExchange = null) => new()
    {
        HostName = "localhost",
        UserName = "guest",
        Password = "guest",
        DeadLetterExchange = deadLetterExchange
    };

    private static (RabbitMQBasicConsumer Consumer, Mock<IChannel> Channel, string BrokerId) CreateConsumer(
        Action<IServiceCollection, string>? configureServices = null,
        string? deadLetterExchange = null)
    {
        var brokerId = Guid.NewGuid().ToString();
        var channel = new Mock<IChannel>();
        channel.Setup(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        channel.Setup(c => c.BasicNackAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        var services = new ServiceCollection();
        services.AddKeyedSingleton<INameResolver>(brokerId, new RabbitMQNameResolver("svc"));
        services.AddKeyedSingleton(brokerId, Settings(deadLetterExchange));
        configureServices?.Invoke(services, brokerId);
        ServiceProvider provider = services.BuildServiceProvider();

        var consumer = new RabbitMQBasicConsumer(
            provider,
            channel.Object,
            brokerId,
            NullLogger<RabbitMQBasicConsumer>.Instance);

        return (consumer, channel, brokerId);
    }

    private static HandlerWrapper Wrapper(
        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> handler)
        => new(typeof(TestMessage), [handler], HandlerWrapper.BuildCreateMessage(typeof(TestMessage)));

    private static IReadOnlyBasicProperties Props(string? correlationId = null, string? replyTo = null)
    {
        var mock = new Mock<IReadOnlyBasicProperties>();
        mock.SetupGet(p => p.CorrelationId).Returns(correlationId);
        mock.SetupGet(p => p.ReplyTo).Returns(replyTo);
        return mock.Object;
    }

    [Fact]
    public async Task Deliver_unknown_exchange_nacks()
    {
        (RabbitMQBasicConsumer consumer, Mock<IChannel> channel, _) = CreateConsumer();

        await consumer.HandleBasicDeliverAsync("ct", 7, false, "Unknown", "rk", Props(), new byte[] { 1 });

        channel.Verify(c => c.BasicNackAsync(7, false, false, It.IsAny<CancellationToken>()), Times.Once);
        channel.Verify(c => c.BasicAckAsync(It.IsAny<ulong>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Deliver_all_handlers_true_acks()
    {
        (RabbitMQBasicConsumer consumer, Mock<IChannel> channel, _) = CreateConsumer(
            configureServices: (services, id) =>
            {
                services.AddKeyedSingleton(id, Wrapper((_, _, _) => Task.FromResult(HandleResult.Ack)));
            });

        await consumer.HandleBasicDeliverAsync("ct", 1, false, nameof(TestMessage), "rk", Props("c"), new byte[] { 1 });

        channel.Verify(c => c.BasicAckAsync(1, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Deliver_handler_false_nacks()
    {
        (RabbitMQBasicConsumer consumer, Mock<IChannel> channel, _) = CreateConsumer(
            configureServices: (services, id) =>
            {
                services.AddKeyedSingleton(id, Wrapper((_, _, _) => Task.FromResult(HandleResult.Retry)));
            });

        await consumer.HandleBasicDeliverAsync("ct", 2, false, nameof(TestMessage), "rk", Props(), new byte[] { 1 });

        channel.Verify(c => c.BasicNackAsync(2, false, true, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Deliver_handler_exception_nacks()
    {
        (RabbitMQBasicConsumer consumer, Mock<IChannel> channel, _) = CreateConsumer(
            configureServices: (services, id) =>
            {
                services.AddKeyedSingleton(id, Wrapper((_, _, _) => throw new InvalidOperationException("boom")));
            });

        await consumer.HandleBasicDeliverAsync("ct", 3, false, nameof(TestMessage), "rk", Props(), new byte[] { 1 });

        channel.Verify(c => c.BasicNackAsync(3, false, true, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Deliver_handler_false_with_dead_letter_exchange_nacks_without_requeue()
    {
        (RabbitMQBasicConsumer consumer, Mock<IChannel> channel, _) = CreateConsumer(
            configureServices: (services, id) =>
            {
                services.AddKeyedSingleton(id, Wrapper((_, _, _) => Task.FromResult(HandleResult.Retry)));
            },
            deadLetterExchange: "dlx");

        await consumer.HandleBasicDeliverAsync("ct", 8, false, nameof(TestMessage), "rk", Props(), new byte[] { 1 });

        channel.Verify(c => c.BasicNackAsync(8, false, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Deliver_sets_tag_from_routing_key()
    {
        string? capturedTag = null;
        (RabbitMQBasicConsumer consumer, Mock<IChannel> channel, _) = CreateConsumer(
            configureServices: (services, id) =>
            {
                services.AddKeyedSingleton(id, Wrapper((_, message, _) =>
                {
                    capturedTag = message.Tag;
                    return Task.FromResult(HandleResult.Ack);
                }));
            });

        await consumer.HandleBasicDeliverAsync(
            "ct",
            9,
            false,
            nameof(TestMessage),
            "orders.created",
            Props(),
            new byte[] { 1 });

        capturedTag.ShouldBe("orders.created");
        channel.Verify(c => c.BasicAckAsync(9, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Deliver_empty_routing_key_sets_empty_tag()
    {
        string? capturedTag = null;
        (RabbitMQBasicConsumer consumer, _, _) = CreateConsumer(
            configureServices: (services, id) =>
            {
                services.AddKeyedSingleton(id, Wrapper((_, message, _) =>
                {
                    capturedTag = message.Tag;
                    return Task.FromResult(HandleResult.Ack);
                }));
            });

        await consumer.HandleBasicDeliverAsync("ct", 10, false, nameof(TestMessage), "", Props(), new byte[] { 1 });

        capturedTag.ShouldBe("");
    }

    [Fact]
    public async Task Deliver_fills_ReplyInfo()
    {
        RabbitMQReplyInfo? captured = null;
        (RabbitMQBasicConsumer consumer, Mock<IChannel> channel, _) = CreateConsumer(
            configureServices: (services, id) =>
            {
                services.AddKeyedSingleton(id, Wrapper((_, _, ctx) =>
                {
                    captured = ctx.ReplyInfo as RabbitMQReplyInfo;
                    return Task.FromResult(HandleResult.Ack);
                }));
            });

        await consumer.HandleBasicDeliverAsync("ct", 4, false, nameof(TestMessage), "rk", Props(replyTo: "reply-q"), new byte[] { 1 });

        captured.ShouldNotBeNull();
        captured!.ReplyTo.ShouldBe("reply-q");
        channel.Verify(c => c.BasicAckAsync(4, false, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ChannelShutdown_suppress_skips_recover()
    {
        var brokerId = Guid.NewGuid().ToString();
        var channel = new Mock<IChannel>();
        var runtime = new RabbitMqRuntime
        {
            SuppressConsumerShutdownRecover = true,
            ConsumerChannel = channel.Object
        };

        var services = new ServiceCollection();
        services.AddKeyedSingleton<INameResolver>(brokerId, new RabbitMQNameResolver("svc"));
        services.AddKeyedSingleton(brokerId, Settings());
        services.AddKeyedSingleton(brokerId, runtime);
        ServiceProvider provider = services.BuildServiceProvider();

        var consumer = new RabbitMQBasicConsumer(
            provider,
            channel.Object,
            brokerId,
            NullLogger<RabbitMQBasicConsumer>.Instance);

        var reason = new ShutdownEventArgs(ShutdownInitiator.Application, 200, "ok");
        await consumer.HandleChannelShutdownAsync(channel.Object, reason);

        // No exception and recover not attempted (no builder required when suppressed)
        runtime.SuppressConsumerShutdownRecover.ShouldBeTrue();
    }

    [Fact]
    public async Task ChannelShutdown_stale_channel_skips_recover()
    {
        var brokerId = Guid.NewGuid().ToString();
        var channel = new Mock<IChannel>();
        var other = new Mock<IChannel>();
        var runtime = new RabbitMqRuntime { ConsumerChannel = other.Object };

        var services = new ServiceCollection();
        services.AddKeyedSingleton<INameResolver>(brokerId, new RabbitMQNameResolver("svc"));
        services.AddKeyedSingleton(brokerId, Settings());
        services.AddKeyedSingleton(brokerId, runtime);
        ServiceProvider provider = services.BuildServiceProvider();

        var consumer = new RabbitMQBasicConsumer(
            provider,
            channel.Object,
            brokerId,
            NullLogger<RabbitMQBasicConsumer>.Instance);

        await consumer.HandleChannelShutdownAsync(channel.Object, new ShutdownEventArgs(ShutdownInitiator.Library, 320, "stale"));
    }

    [Fact]
    public async Task ChannelShutdown_missing_builder_skips_recover()
    {
        var brokerId = Guid.NewGuid().ToString();
        var channel = new Mock<IChannel>();
        var runtime = new RabbitMqRuntime { ConsumerChannel = channel.Object };

        var services = new ServiceCollection();
        services.AddKeyedSingleton<INameResolver>(brokerId, new RabbitMQNameResolver("svc"));
        services.AddKeyedSingleton(brokerId, Settings());
        services.AddKeyedSingleton(brokerId, runtime);
        ServiceProvider provider = services.BuildServiceProvider();

        var consumer = new RabbitMQBasicConsumer(
            provider,
            channel.Object,
            brokerId,
            NullLogger<RabbitMQBasicConsumer>.Instance);

        await consumer.HandleChannelShutdownAsync(channel.Object, new ShutdownEventArgs(ShutdownInitiator.Peer, 541, "no-builder"));
    }
}
