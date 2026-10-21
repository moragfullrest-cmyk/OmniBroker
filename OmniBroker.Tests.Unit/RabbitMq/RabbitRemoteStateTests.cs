using Microsoft.Extensions.DependencyInjection;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ;
using OmniBroker.RabbitMQ.Implementations;
using OmniBroker.RabbitMQ.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Shouldly;

namespace OmniBroker.Tests.Unit.RabbitMq;

public sealed class RabbitRemoteStateTests
{
    [Fact]
    public async Task ValidateRemoteBrokerState_channel_open_does_not_declare_topology()
    {
        var channel = new Mock<IChannel>();
        SetupChannelLifecycle(channel);
        Mock<IConnection> connection = ConnectionWithChannel(channel.Object);
        (RabbitMQExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(connection.Object);

        await extension.ValidateRemoteBrokerState(provider, builder);

        channel.Verify(c => c.ExchangeDeclareAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Never);
        channel.Verify(c => c.QueueDeclareAsync(
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ValidateRemoteBrokerState_channel_refusal_throws_without_password()
    {
        var connection = new Mock<IConnection>();
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("channel max"));
        (RabbitMQExtension extension, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(
            connection.Object,
            password: "secret-password");

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(
            () => extension.ValidateRemoteBrokerState(provider, builder));

        ex.BrokerName.ShouldBe(builder.BrokerName);
        ex.Message.ShouldContain("refused a channel");
        ex.Message.ShouldNotContain("secret-password");
    }

    [Fact]
    public async Task StartAsync_access_refused_wraps_before_consumers()
    {
        await AssertDeclareConflictStopsBeforeConsumers(
            Constants.AccessRefused,
            "ACCESS_REFUSED - insufficient permissions");
    }

    [Fact]
    public async Task StartAsync_precondition_failed_wraps_before_consumers()
    {
        await AssertDeclareConflictStopsBeforeConsumers(
            Constants.PreconditionFailed,
            "PRECONDITION_FAILED - inequivalent arg");
    }

    private static async Task AssertDeclareConflictStopsBeforeConsumers(ushort replyCode, string replyText)
    {
        var probe = new Mock<IChannel>();
        SetupChannelLifecycle(probe);
        var idle = new Mock<IChannel>();
        SetupChannelLifecycle(idle);
        var declare = new Mock<IChannel>();
        SetupChannelLifecycle(declare);
        declare.Setup(c => c.ExchangeDeclareAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationInterruptedException(
                new ShutdownEventArgs(ShutdownInitiator.Peer, replyCode, replyText, (object?)null, CancellationToken.None)));

        var channels = new Queue<IChannel>(new[] { probe.Object, idle.Object, declare.Object });
        var connection = new Mock<IConnection>();
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .Returns((CreateChannelOptions _, CancellationToken _) => Task.FromResult(channels.Dequeue()));

        (RabbitMQExtension _, BrokerOptionsBuilder builder, ServiceProvider provider) = Create(connection.Object);
        builder.AddConsumerFor<TestMessage>((TestMessage _) => Task.FromResult(HandleResult.Ack));
        var hosted = new RabbitMqBrokerHostedService(provider, builder.BrokerName);

        BrokerStateException ex = await Should.ThrowAsync<BrokerStateException>(() => hosted.StartAsync(CancellationToken.None));

        ex.BrokerName.ShouldBe(builder.BrokerName);
        ex.Message.ShouldContain(replyText);
        ex.InnerException.ShouldBeOfType<OperationInterruptedException>();
        declare.Verify(c => c.BasicConsumeAsync(
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<string>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<IAsyncBasicConsumer>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    private static (RabbitMQExtension Extension, BrokerOptionsBuilder Builder, ServiceProvider Provider) Create(
        IConnection connection,
        string password = "guest")
    {
        var builder = new BrokerOptionsBuilder { BrokerName = "rabbit" };
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = password,
            VirtualHost = "/"
        });
        var extension = (RabbitMQExtension)builder.Extension!;
        var runtime = new RabbitMqRuntime { Connection = connection };
        var services = new ServiceCollection();
        services.AddKeyedSingleton(builder.BrokerName, runtime);
        services.AddKeyedSingleton(builder.BrokerName, builder);
        services.AddKeyedSingleton<INameResolver>(builder.BrokerName, new RabbitMQNameResolver(builder.BrokerName));
        return (extension, builder, services.BuildServiceProvider());
    }

    private static void SetupChannelLifecycle(Mock<IChannel> channel)
    {
        channel.Setup(c => c.CloseAsync(It.IsAny<ushort>(), It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        channel.Setup(c => c.DisposeAsync()).Returns(ValueTask.CompletedTask);
    }

    private static Mock<IConnection> ConnectionWithChannel(IChannel channel)
    {
        var connection = new Mock<IConnection>();
        connection.Setup(c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(channel);
        return connection;
    }
}
