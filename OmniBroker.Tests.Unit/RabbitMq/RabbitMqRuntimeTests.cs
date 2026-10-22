using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ;
using OmniBroker.RabbitMQ.Implementations;
using OmniBroker.RabbitMQ.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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
        var builder = new BrokerOptionsBuilder { BrokerName = "di" };
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
        services.AddKeyedSingleton(builder.BrokerName, runtime);
        ServiceProvider provider = services.BuildServiceProvider();

        var hosted = new RabbitMqBrokerHostedService(provider, builder.BrokerName);
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
        services.AddKeyedSingleton(builder.BrokerName, runtime);
        ServiceProvider provider = services.BuildServiceProvider();

        Task retry = extension.RetryStartAsync(provider, builder, runtime.StoppingToken);
        await logger.RetryLogged.Task.WaitAsync(TimeSpan.FromSeconds(2));
        runtime.SignalStop();
        await retry.WaitAsync(TimeSpan.FromSeconds(2));

        runtime.ReconnectLock.CurrentCount.ShouldBe(1);
    }

    [Fact]
    public async Task Recover_open_connection_without_consumers_does_not_reset()
    {
        BrokerOptionsBuilder builder = RabbitBuilder();
        var extension = (RabbitMQExtension)builder.Extension!;
        Mock<IConnection> connection = OpenConnection();
        var runtime = new RabbitMqRuntime { Connection = connection.Object };
        ServiceProvider provider = ProviderWithRuntime(builder, runtime);

        await extension.RecoverAsync(provider, builder);

        runtime.Connection.ShouldBeSameAs(connection.Object);
        runtime.ConsumerChannel.ShouldBeNull();
        VerifyConnectionWasNotClosed(connection);
    }

    [Fact]
    public async Task Recover_second_signal_does_not_reset_open_publisher_connection()
    {
        BrokerOptionsBuilder builder = RabbitBuilder();
        var extension = (RabbitMQExtension)builder.Extension!;
        Mock<IConnection> connection = OpenConnection();
        var runtime = new RabbitMqRuntime { Connection = connection.Object };
        ServiceProvider provider = ProviderWithRuntime(builder, runtime);

        await runtime.ReconnectLock.WaitAsync();
        Task recover = extension.RecoverAsync(provider, builder);
        await Task.Delay(50);
        recover.IsCompleted.ShouldBeFalse();
        runtime.ReconnectLock.Release();

        await recover.WaitAsync(TimeSpan.FromSeconds(2));
        runtime.Connection.ShouldBeSameAs(connection.Object);
        runtime.ReconnectLock.CurrentCount.ShouldBe(1);
        VerifyConnectionWasNotClosed(connection);
    }

    [Fact]
    public async Task RetryStart_consume_failure_retries_and_drops_failed_channel()
    {
        var settings = new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };
        var builder = new BrokerOptionsBuilder { BrokerName = "svc" };
        builder.UseRabbitMq(settings);
        builder.AddConsumerFor<TestMessage>((TestMessage _) => Task.FromResult(HandleResult.Ack));
        var extension = (RabbitMQExtension)builder.Extension!;

        var consumed = new List<Mock<IChannel>>();
        int attempts = 0;
        var connection = new Mock<IConnection>();
        connection.SetupGet(c => c.IsOpen).Returns(true);
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .Returns((CreateChannelOptions _, CancellationToken _) =>
            {
                var channel = new Mock<IChannel>();
                SetupBrokerChannel(channel, () =>
                {
                    consumed.Add(channel);
                    int n = Interlocked.Increment(ref attempts);
                    if (n == 1)
                        throw new InvalidOperationException("consume failed");

                    return Task.FromResult("tag");
                });
                return Task.FromResult(channel.Object);
            });

        var runtime = new RabbitMqRuntime { Connection = connection.Object };
        var services = new ServiceCollection();
        await extension.SetupConsumers(services, builder);
        services.AddSingleton<ILogger<RabbitMQExtension>>(new StopAfterSecondError(runtime));
        services.AddSingleton<ILogger<RabbitMQBasicConsumer>>(NullLogger<RabbitMQBasicConsumer>.Instance);
        services.AddKeyedSingleton<INameResolver>(builder.BrokerName, new RabbitMQNameResolver(builder.BrokerName));
        services.AddKeyedSingleton(builder.BrokerName, settings);
        services.AddKeyedSingleton(builder.BrokerName, runtime);
        ServiceProvider provider = services.BuildServiceProvider();

        try
        {
            await extension.RetryStartAsync(provider, builder, runtime.StoppingToken)
                .WaitAsync(TimeSpan.FromSeconds(5));

            attempts.ShouldBe(2);
            runtime.ConsumerChannel.ShouldBeSameAs(consumed[1].Object);
            consumed[0].Verify(c => c.CloseAsync(
                It.IsAny<ushort>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()), Times.Once);
            consumed[0].Verify(c => c.DisposeAsync(), Times.Once);
            consumed[1].Verify(c => c.CloseAsync(
                It.IsAny<ushort>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()), Times.Never);
        }
        catch (TimeoutException)
        {
            runtime.SignalStop();
            throw;
        }
        finally
        {
            if (runtime.ChannelPool is not null)
                await runtime.ChannelPool.DisposeAsync();
        }
    }

    [Fact]
    public async Task Connection_shutdown_handler_enters_recover()
    {
        BrokerOptionsBuilder builder = RabbitBuilder();
        var extension = (RabbitMQExtension)builder.Extension!;
        Mock<IConnection> connection = OpenConnection();
        var runtime = new RabbitMqRuntime { Connection = connection.Object };
        ServiceProvider provider = ProviderWithRuntime(builder, runtime);
        extension.AttachConnectionShutdown(connection.Object, runtime, provider, builder);
        AsyncEventHandler<ShutdownEventArgs> handler = runtime.ConnectionShutdown!;

        await runtime.ReconnectLock.WaitAsync();
        Task shutdown = handler(connection.Object, new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "connection down"));
        await Task.Delay(50);
        shutdown.IsCompleted.ShouldBeFalse();
        runtime.ReconnectLock.Release();

        await shutdown.WaitAsync(TimeSpan.FromSeconds(2));
        runtime.Connection.ShouldBeSameAs(connection.Object);
        VerifyConnectionWasNotClosed(connection);
    }

    [Fact]
    public async Task Connection_shutdown_handler_skips_recover_when_suppressed_or_stopping()
    {
        BrokerOptionsBuilder builder = RabbitBuilder();
        var extension = (RabbitMQExtension)builder.Extension!;
        Mock<IConnection> connection = OpenConnection();
        var runtime = new RabbitMqRuntime { Connection = connection.Object };
        ServiceProvider provider = ProviderWithRuntime(builder, runtime);
        extension.AttachConnectionShutdown(connection.Object, runtime, provider, builder);
        AsyncEventHandler<ShutdownEventArgs> handler = runtime.ConnectionShutdown!;
        var reason = new ShutdownEventArgs(ShutdownInitiator.Application, 200, "stop");

        runtime.SuppressConsumerShutdownRecover = true;
        await runtime.ReconnectLock.WaitAsync();
        await handler(connection.Object, reason).WaitAsync(TimeSpan.FromSeconds(2));

        runtime.SuppressConsumerShutdownRecover = false;
        runtime.SignalStop();
        await handler(connection.Object, reason).WaitAsync(TimeSpan.FromSeconds(2));
        runtime.ReconnectLock.Release();

        runtime.Connection.ShouldBeSameAs(connection.Object);
        VerifyConnectionWasNotClosed(connection);
    }

    [Fact]
    public async Task Reset_unsubscribes_connection_shutdown_before_close()
    {
        BrokerOptionsBuilder builder = RabbitBuilder();
        var extension = (RabbitMQExtension)builder.Extension!;
        AsyncEventHandler<ShutdownEventArgs>? subscribed = null;
        int raisedOnClose = 0;
        var connection = new Mock<IConnection>();
        connection.SetupGet(c => c.IsOpen).Returns(true);
        connection.SetupAdd(c => c.ConnectionShutdownAsync += It.IsAny<AsyncEventHandler<ShutdownEventArgs>>())
            .Callback<AsyncEventHandler<ShutdownEventArgs>>(handler => subscribed += handler);
        connection.SetupRemove(c => c.ConnectionShutdownAsync -= It.IsAny<AsyncEventHandler<ShutdownEventArgs>>())
            .Callback<AsyncEventHandler<ShutdownEventArgs>>(handler => subscribed -= handler);
        connection.Setup(c => c.CloseAsync(
                It.IsAny<ushort>(),
                It.IsAny<string>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (subscribed is not null)
                    raisedOnClose++;
                return Task.CompletedTask;
            });

        var runtime = new RabbitMqRuntime { Connection = connection.Object };
        ServiceProvider provider = ProviderWithRuntime(builder, runtime);
        extension.AttachConnectionShutdown(connection.Object, runtime, provider, builder);
        subscribed.ShouldNotBeNull();

        await runtime.ResetConnectionCoreAsync();

        raisedOnClose.ShouldBe(0);
        runtime.Connection.ShouldBeNull();
        runtime.ConnectionShutdown.ShouldBeNull();
        runtime.SuppressConsumerShutdownRecover.ShouldBeTrue();
    }

    private static BrokerOptionsBuilder RabbitBuilder()
    {
        var builder = new BrokerOptionsBuilder { BrokerName = "svc" };
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        });
        return builder;
    }

    private static Mock<IConnection> OpenConnection()
    {
        var connection = new Mock<IConnection>();
        connection.SetupGet(c => c.IsOpen).Returns(true);
        connection.Setup(c => c.CloseAsync(
                It.IsAny<ushort>(),
                It.IsAny<string>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        return connection;
    }

    private static void VerifyConnectionWasNotClosed(Mock<IConnection> connection)
    {
        connection.Verify(c => c.CloseAsync(
            It.IsAny<ushort>(),
            It.IsAny<string>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    private static void SetupBrokerChannel(Mock<IChannel> channel, Func<Task<string>> consume)
    {
        SetupChannelLifecycle(channel);
        channel.SetupGet(c => c.IsOpen).Returns(true);
        channel.Setup(c => c.BasicQosAsync(
                It.IsAny<uint>(),
                It.IsAny<ushort>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        channel.Setup(c => c.ExchangeDeclareAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        channel.Setup(c => c.QueueDeclareAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueueDeclareOk("reply-q", 0, 0));
        channel.Setup(c => c.QueueBindAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        channel.Setup(c => c.BasicConsumeAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<IAsyncBasicConsumer>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => consume());
    }

    private static ServiceProvider ProviderWithRuntime(BrokerOptionsBuilder builder, RabbitMqRuntime runtime)
    {
        var services = new ServiceCollection();
        services.AddKeyedSingleton(builder.BrokerName, runtime);
        return services.BuildServiceProvider();
    }

    private sealed class StopAfterSecondError(RabbitMqRuntime runtime) : ILogger<RabbitMQExtension>
    {
        private int _errors;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Error && Interlocked.Increment(ref _errors) >= 2)
                runtime.SignalStop();
        }
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
