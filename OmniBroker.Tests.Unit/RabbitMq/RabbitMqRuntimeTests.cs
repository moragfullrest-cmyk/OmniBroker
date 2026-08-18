using Microsoft.Extensions.DependencyInjection;
using Moq;
using OmniBroker.RabbitMQ;
using OmniBroker.RabbitMQ.ServiceSetup;
using RabbitMQ.Client;
using Shouldly;

namespace OmniBroker.Tests.Unit.RabbitMq;

public sealed class RabbitMqRuntimeTests
{
    private static void SetupChannelLifecycle(Mock<IChannel> channel)
    {
        channel.Setup(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        channel.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
    }

    private static Mock<IConnection> ConnectionWithChannel(Mock<IChannel> channel)
    {
        var connection = new Mock<IConnection>();
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channel.Object);
        connection.Setup(c => c.CloseAsync(
                It.IsAny<ushort>(),
                It.IsAny<string>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return connection;
    }

    [Fact]
    public async Task ResetConnectionAsync_keeps_pool_and_disposes_idle_channel()
    {
        var idle = new Mock<IChannel>();
        SetupChannelLifecycle(idle);

        var consumer = new Mock<IChannel>();
        SetupChannelLifecycle(consumer);
        consumer.SetupGet(c => c.IsOpen).Returns(true);

        Mock<IConnection> connection = ConnectionWithChannel(idle);
        await using var pool = new ConcurrentObjectPool<IChannel>(
            _ => Task.FromResult(idle.Object),
            maxSize: 2);

        var runtime = new RabbitMqRuntime
        {
            Connection = connection.Object,
            ChannelPool = pool,
            ConsumerChannel = consumer.Object
        };

        IChannel item = await pool.GetAsync();
        pool.Return(item);

        await runtime.ResetConnectionAsync();

        runtime.ChannelPool.ShouldBeSameAs(pool);
        runtime.Connection.ShouldBeNull();
        runtime.ConsumerChannel.ShouldBeNull();
        idle.Verify(c => c.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ResetConnectionAsync_same_pool_creates_channel_via_new_connection()
    {
        var oldChannel = new Mock<IChannel>();
        SetupChannelLifecycle(oldChannel);
        Mock<IConnection> oldConnection = ConnectionWithChannel(oldChannel);

        var runtime = new RabbitMqRuntime { Connection = oldConnection.Object };
        await using var pool = new ConcurrentObjectPool<IChannel>(
            ct => runtime.RequireConnection().CreateChannelAsync(cancellationToken: ct),
            maxSize: 2);
        runtime.ChannelPool = pool;

        IChannel idle = await pool.GetAsync();
        pool.Return(idle);

        await runtime.ResetConnectionAsync();
        runtime.ChannelPool.ShouldBeSameAs(pool);

        var newChannel = new Mock<IChannel>();
        SetupChannelLifecycle(newChannel);
        Mock<IConnection> newConnection = ConnectionWithChannel(newChannel);
        runtime.Connection = newConnection.Object;

        IChannel created = await pool.GetAsync();

        created.ShouldBeSameAs(newChannel.Object);
        oldChannel.Verify(c => c.DisposeAsync(), Times.Once);
        oldConnection.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
        newConnection.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task Keyed_IConnection_is_transient_and_follows_runtime_swap()
    {
        var builder = new BrokerOptionsBuilder { SetupName = "di" };
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        });
        var extension = (RabbitMQExtension)builder.Extension!;

        var services = new ServiceCollection();
        await extension.SetupInfrastructure(services, builder);
        ServiceProvider provider = services.BuildServiceProvider();

        RabbitMqRuntime runtime = provider.GetRequiredKeyedService<RabbitMqRuntime>(builder.BrokerId);
        var first = Mock.Of<IConnection>();
        runtime.Connection = first;

        provider.GetRequiredKeyedService<IConnection>(builder.BrokerId).ShouldBeSameAs(first);

        var second = Mock.Of<IConnection>();
        runtime.Connection = second;

        provider.GetRequiredKeyedService<IConnection>(builder.BrokerId).ShouldBeSameAs(second);
    }
}
