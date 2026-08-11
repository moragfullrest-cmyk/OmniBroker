using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.Implementations;
using OmniBroker.RabbitMQ.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Shouldly;

namespace OmniBroker.Tests.Unit.RabbitMq;

public sealed class RabbitMQBasicProducerTests
{
    private sealed class TestRabbitClientException(string message) : RabbitMQClientException(message);

    private static (RabbitMQBasicProducer<TestMessage> Sut, Mock<IChannel> Channel, ConcurrentObjectPool<IChannel> Pool) CreateSut(
        Action<Mock<IChannel>>? configure = null)
    {
        var channel = new Mock<IChannel>();
        channel.SetupGet(c => c.IsClosed).Returns(false);
        channel.SetupGet(c => c.IsOpen).Returns(true);
        channel.Setup(c => c.BasicPublishAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        configure?.Invoke(channel);

        var pool = new ConcurrentObjectPool<IChannel>(_ => Task.FromResult(channel.Object), maxSize: 4);
        var resolver = new Mock<INameResolver>();
        resolver.Setup(r => r.ResolveOutboundName(typeof(TestMessage))).Returns(nameof(TestMessage));

        var sut = new RabbitMQBasicProducer<TestMessage>(
            NullLogger<RabbitMQBasicProducer<TestMessage>>.Instance,
            pool,
            resolver.Object);

        return (sut, channel, pool);
    }

    [Fact]
    public async Task Publish_null_message_throws()
    {
        (RabbitMQBasicProducer<TestMessage> sut, _, ConcurrentObjectPool<IChannel> pool) = CreateSut();
        await using (pool)
        {
            await Should.ThrowAsync<ArgumentNullException>(() => sut.Publish(null!));
        }
    }

    [Fact]
    public async Task Publish_success_returns_true_and_returns_channel()
    {
        (RabbitMQBasicProducer<TestMessage> sut, Mock<IChannel> channel, ConcurrentObjectPool<IChannel> pool) = CreateSut();
        await using (pool)
        {
            bool result = await sut.Publish(new TestMessage { Body = [1], Tag = "rk" });
            IChannel again = await pool.GetAsync();

            result.ShouldBeTrue();
            again.ShouldBeSameAs(channel.Object);
            channel.Verify(c => c.BasicPublishAsync(
                nameof(TestMessage),
                "rk",
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }
    }

    [Fact]
    public async Task Publish_destination_overrides_resolver()
    {
        (RabbitMQBasicProducer<TestMessage> sut, Mock<IChannel> channel, ConcurrentObjectPool<IChannel> pool) = CreateSut();
        await using (pool)
        {
            await sut.Publish(new TestMessage { Body = [1], Tag = "rk" }, new PublishOptions(null, null, "custom-ex"));

            channel.Verify(c => c.BasicPublishAsync(
                "custom-ex",
                "rk",
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }
    }

    [Fact]
    public async Task Publish_empty_tag_uses_empty_routing_key()
    {
        (RabbitMQBasicProducer<TestMessage> sut, Mock<IChannel> channel, ConcurrentObjectPool<IChannel> pool) = CreateSut();
        await using (pool)
        {
            await sut.Publish(new TestMessage { Body = [1], Tag = "" });

            channel.Verify(c => c.BasicPublishAsync(
                It.IsAny<string>(),
                "",
                true,
                It.IsAny<BasicProperties>(),
                It.IsAny<ReadOnlyMemory<byte>>(),
                It.IsAny<CancellationToken>()), Times.Once);
        }
    }

    [Fact]
    public async Task Publish_PublishException_returns_false()
    {
        (RabbitMQBasicProducer<TestMessage> sut, _, ConcurrentObjectPool<IChannel> pool) = CreateSut(channel =>
        {
            channel.Setup(c => c.BasicPublishAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<BasicProperties>(),
                    It.IsAny<ReadOnlyMemory<byte>>(),
                    It.IsAny<CancellationToken>()))
                .Throws(new PublishException(1UL, false));
        });
        await using (pool)
        {
            bool result = await sut.Publish(new TestMessage { Body = [1] });

            result.ShouldBeFalse();
        }
    }

    [Fact]
    public async Task Publish_RabbitMQClientException_returns_false()
    {
        (RabbitMQBasicProducer<TestMessage> sut, _, ConcurrentObjectPool<IChannel> pool) = CreateSut(channel =>
        {
            channel.Setup(c => c.BasicPublishAsync(
                    It.IsAny<string>(),
                    It.IsAny<string>(),
                    It.IsAny<bool>(),
                    It.IsAny<BasicProperties>(),
                    It.IsAny<ReadOnlyMemory<byte>>(),
                    It.IsAny<CancellationToken>()))
                .Throws(new TestRabbitClientException("client error"));
        });
        await using (pool)
        {
            bool result = await sut.Publish(new TestMessage { Body = [1] });

            result.ShouldBeFalse();
        }
    }

    [Fact]
    public async Task Publish_closed_channel_returns_false_and_discards()
    {
        int created = 0;
        var closed = new Mock<IChannel>();
        closed.SetupGet(c => c.IsClosed).Returns(true);
        closed.SetupGet(c => c.IsOpen).Returns(false);

        var open = new Mock<IChannel>();
        open.SetupGet(c => c.IsClosed).Returns(false);
        open.SetupGet(c => c.IsOpen).Returns(true);

        var pool = new ConcurrentObjectPool<IChannel>(_ =>
        {
            int n = Interlocked.Increment(ref created);
            return Task.FromResult(n == 1 ? closed.Object : open.Object);
        }, maxSize: 2);

        var resolver = new Mock<INameResolver>();
        resolver.Setup(r => r.ResolveOutboundName(typeof(TestMessage))).Returns(nameof(TestMessage));
        var sut = new RabbitMQBasicProducer<TestMessage>(
            NullLogger<RabbitMQBasicProducer<TestMessage>>.Instance,
            pool,
            resolver.Object);

        await using (pool)
        {
            bool result = await sut.Publish(new TestMessage { Body = [1] });
            IChannel next = await pool.GetAsync();

            result.ShouldBeFalse();
            next.ShouldBeSameAs(open.Object);
            created.ShouldBe(2);
        }
    }
}
