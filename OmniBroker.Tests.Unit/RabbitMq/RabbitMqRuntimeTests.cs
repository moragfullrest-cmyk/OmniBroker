using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
    public async Task SetupInfrastructure_does_not_register_IConnection()
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

        services.Any(d => d.ServiceType == typeof(IConnection)).ShouldBeFalse();
    }

    [Fact]
    public async Task ResetConnectionAsync_waits_for_reconnect_lock()
    {
        var runtime = new RabbitMqRuntime
        {
            Connection = Mock.Of<IConnection>()
        };
        await runtime.ReconnectLock.WaitAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        await Should.ThrowAsync<OperationCanceledException>(() => runtime.ResetConnectionAsync(cts.Token));

        runtime.Connection.ShouldNotBeNull();
        runtime.ReconnectLock.Release();
    }

    [Fact]
    public async Task StopAsync_signals_stop_and_resets_under_lock()
    {
        var idle = new Mock<IChannel>();
        SetupChannelLifecycle(idle);
        var consumer = new Mock<IChannel>();
        SetupChannelLifecycle(consumer);
        consumer.SetupGet(c => c.IsOpen).Returns(true);
        Mock<IConnection> connection = ConnectionWithChannel(idle);

        BrokerOptionsBuilder builder = RabbitBuilder();
        await using var pool = new ConcurrentObjectPool<IChannel>(_ => Task.FromResult(idle.Object), maxSize: 1);
        var runtime = new RabbitMqRuntime
        {
            Connection = connection.Object,
            ChannelPool = pool,
            ConsumerChannel = consumer.Object
        };

        var services = new ServiceCollection();
        services.AddKeyedSingleton(builder.BrokerId, runtime);
        ServiceProvider provider = services.BuildServiceProvider();

        var hosted = new RabbitMqBrokerHostedService(provider, builder.BrokerId);
        await hosted.StopAsync(CancellationToken.None);

        runtime.StoppingToken.IsCancellationRequested.ShouldBeTrue();
        runtime.Connection.ShouldBeNull();
        runtime.ConsumerChannel.ShouldBeNull();
        runtime.ReconnectLock.CurrentCount.ShouldBe(1);
    }

    [Fact]
    public async Task Recover_after_stop_does_not_reset_connection()
    {
        BrokerOptionsBuilder builder = RabbitBuilder();
        var extension = (RabbitMQExtension)builder.Extension!;
        IConnection connection = Mock.Of<IConnection>(c => c.IsOpen == false);
        var runtime = new RabbitMqRuntime
        {
            Connection = connection,
            ConsumerChannel = Mock.Of<IChannel>(c => c.IsOpen == false)
        };
        ServiceProvider provider = ProviderWithRuntime(builder, runtime);
        runtime.SignalStop();

        await extension.RecoverAsync(provider, builder);

        runtime.Connection.ShouldBeSameAs(connection);
        runtime.ReconnectLock.CurrentCount.ShouldBe(1);
    }

    [Fact]
    public async Task Recover_aborts_when_stop_is_signaled_while_lock_is_held()
    {
        BrokerOptionsBuilder builder = RabbitBuilder();
        var extension = (RabbitMQExtension)builder.Extension!;
        IConnection connection = Mock.Of<IConnection>(c => c.IsOpen == false);
        var runtime = new RabbitMqRuntime
        {
            Connection = connection,
            ConsumerChannel = Mock.Of<IChannel>(c => c.IsOpen == false)
        };
        ServiceProvider provider = ProviderWithRuntime(builder, runtime);

        await runtime.ReconnectLock.WaitAsync();
        Task recover = extension.RecoverAsync(provider, builder);
        runtime.SignalStop();
        runtime.ReconnectLock.Release();

        await recover.WaitAsync(TimeSpan.FromSeconds(2));
        runtime.Connection.ShouldBeSameAs(connection);
        runtime.ReconnectLock.CurrentCount.ShouldBe(1);
    }

    [Fact]
    public async Task Recover_when_connection_is_open_releases_lock()
    {
        BrokerOptionsBuilder builder = RabbitBuilder();
        var extension = (RabbitMQExtension)builder.Extension!;
        var runtime = new RabbitMqRuntime
        {
            Connection = Mock.Of<IConnection>(c => c.IsOpen == true),
            ConsumerChannel = Mock.Of<IChannel>(c => c.IsOpen == true)
        };
        ServiceProvider provider = ProviderWithRuntime(builder, runtime);

        await extension.RecoverAsync(provider, builder);

        runtime.ReconnectLock.CurrentCount.ShouldBe(1);
        runtime.SuppressConsumerShutdownRecover.ShouldBeFalse();
    }

    [Fact]
    public async Task RetryStart_delay_ends_when_host_stops()
    {
        BrokerOptionsBuilder builder = RabbitBuilder();
        var extension = (RabbitMQExtension)builder.Extension!;
        var logger = new RetrySignalLogger();
        var runtime = new RabbitMqRuntime
        {
            Connection = Mock.Of<IConnection>()
        };

        var services = new ServiceCollection();
        services.AddSingleton<ILogger<RabbitMQExtension>>(logger);
        services.AddKeyedSingleton(builder.BrokerId, runtime);
        ServiceProvider provider = services.BuildServiceProvider();

        Task retry = extension.RetryStartAsync(provider, builder, runtime.StoppingToken);
        await logger.RetryLogged.Task.WaitAsync(TimeSpan.FromSeconds(2));
        runtime.SignalStop();
        await retry.WaitAsync(TimeSpan.FromSeconds(2));

        runtime.ReconnectLock.CurrentCount.ShouldBe(1);
    }

    private static BrokerOptionsBuilder RabbitBuilder()
    {
        var builder = new BrokerOptionsBuilder { SetupName = "svc" };
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        });
        return builder;
    }

    private static ServiceProvider ProviderWithRuntime(BrokerOptionsBuilder builder, RabbitMqRuntime runtime)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(builder.BrokerId, runtime);
        return services.BuildServiceProvider();
    }

    private sealed class RetrySignalLogger : ILogger<RabbitMQExtension>
    {
        public TaskCompletionSource RetryLogged { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Error)
                RetryLogged.TrySetResult();
        }
    }
}
