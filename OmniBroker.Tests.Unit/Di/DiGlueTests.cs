using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.Implementations;
using OmniBroker.Kafka.ServiceSetup;
using OmniBroker.RabbitMQ;
using OmniBroker.RabbitMQ.Implementations;
using OmniBroker.RabbitMQ.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using RabbitMQ.Client;
using Shouldly;

namespace OmniBroker.Tests.Unit.Di;

public sealed class DiGlueTests
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
        return connection;
    }

    [Fact]
    public void UseRabbitMq_sets_extension_and_default_resolver()
    {
        var builder = new BrokerOptionsBuilder { SetupName = "prefix" };

        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        });

        builder.Extension.ShouldBeOfType<RabbitMQExtension>();
        builder.NameResolver.ShouldBeOfType<RabbitMQNameResolver>();
        builder.NameResolver!.ResolveInboundName(typeof(TestMessage)).ShouldBe("prefix_TestMessage");
    }

    [Fact]
    public void UseKafka_sets_extension_and_default_resolver()
    {
        var builder = new BrokerOptionsBuilder();

        builder.UseKafka(new KafkaSettings { Hosts = "localhost:9092" });

        builder.Extension.ShouldBeOfType<KafkaExtension>();
        builder.NameResolver.ShouldBeOfType<KafkaNameResolver>();
    }

    [Fact]
    public async Task Kafka_SetupInfrastructure_without_NameResolver_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseKafka(new KafkaSettings { Hosts = "localhost:9092" });
        builder.NameResolver = null;
        var extension = (KafkaExtension)builder.Extension!;

        await Should.ThrowAsync<ArgumentNullException>(() =>
            extension.SetupInfrastructure(new ServiceCollection(), builder));
    }

    [Fact]
    public void RabbitMqRuntime_Require_before_start_throws()
    {
        var runtime = new RabbitMqRuntime();

        Should.Throw<InvalidOperationException>(() => runtime.RequireConnection());
        Should.Throw<InvalidOperationException>(() => runtime.RequireChannelPool());
    }

    [Fact]
    public void RabbitMqRuntime_Require_after_start_returns_values()
    {
        var connection = Mock.Of<IConnection>();
        var pool = new ConcurrentObjectPool<IChannel>(_ => Task.FromResult(Mock.Of<IChannel>()), maxSize: 1);
        var runtime = new RabbitMqRuntime
        {
            Connection = connection,
            ChannelPool = pool
        };

        runtime.RequireConnection().ShouldBeSameAs(connection);
        runtime.RequireChannelPool().ShouldBeSameAs(pool);
    }

    [Fact]
    public async Task TopologyDeclarer_EnsureProducersDeclared_declares_exchanges()
    {
        var channel = new Mock<IChannel>();
        SetupChannelLifecycle(channel);
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

        Mock<IConnection> connection = ConnectionWithChannel(channel);
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        });
        builder.AddProducerFor<TestMessage>();
        var resolver = new RabbitMQNameResolver(builder.SetupName);

        await TopologyDeclarer.EnsureProducersDeclared(connection.Object, resolver, builder);

        channel.Verify(c => c.ExchangeDeclareAsync(
            nameof(TestMessage),
            ExchangeType.Topic,
            true,
            false,
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TopologyDeclarer_EnsureConsumersDeclared_declares_queue_and_bind()
    {
        var channel = new Mock<IChannel>();
        SetupChannelLifecycle(channel);
        channel.Setup(c => c.QueueDeclareAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<bool>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueueDeclareOk("q", 0, 0));
        channel.Setup(c => c.QueueBindAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        Mock<IConnection> connection = ConnectionWithChannel(channel);
        var builder = new BrokerOptionsBuilder { SetupName = "svc" };
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        });
        builder.AddConsumerFor<TestMessage>((TestMessage _) => Task.FromResult(true));
        var resolver = new RabbitMQNameResolver(builder.SetupName);

        await TopologyDeclarer.EnsureConsumersDeclared(connection.Object, resolver, builder);

        channel.Verify(c => c.QueueDeclareAsync(
            "svc_TestMessage",
            false,
            false,
            false,
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
        channel.Verify(c => c.QueueBindAsync(
            "svc_TestMessage",
            nameof(TestMessage),
            "",
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TopologyDeclarer_EnsureConsumersDeclared_with_dead_letter_exchange_declares_dlx_and_queue_args()
    {
        var channel = new Mock<IChannel>();
        SetupChannelLifecycle(channel);
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
            .ReturnsAsync(new QueueDeclareOk("q", 0, 0));
        channel.Setup(c => c.QueueBindAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        Mock<IConnection> connection = ConnectionWithChannel(channel);
        var builder = new BrokerOptionsBuilder { SetupName = "svc" };
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest",
            DeadLetterExchange = "dlx"
        });
        builder.AddConsumerFor<TestMessage>((TestMessage _) => Task.FromResult(true));
        var resolver = new RabbitMQNameResolver(builder.SetupName);

        await TopologyDeclarer.EnsureConsumersDeclared(connection.Object, resolver, builder, "dlx");

        channel.Verify(c => c.ExchangeDeclareAsync(
            "dlx",
            ExchangeType.Fanout,
            true,
            false,
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
        channel.Verify(c => c.QueueDeclareAsync(
            "dlx",
            true,
            false,
            false,
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
        channel.Verify(c => c.QueueBindAsync(
            "dlx",
            "dlx",
            "",
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
        channel.Verify(c => c.QueueDeclareAsync(
            "svc_TestMessage",
            false,
            false,
            false,
            It.Is<IDictionary<string, object?>>(args =>
                args != null
                && args.ContainsKey("x-dead-letter-exchange")
                && Equals(args["x-dead-letter-exchange"], "dlx")),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task TopologyDeclarer_EnsureRpcDeclared_with_dead_letter_exchange_skips_reply_queue_args()
    {
        var channel = new Mock<IChannel>();
        SetupChannelLifecycle(channel);
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
            .ReturnsAsync(new QueueDeclareOk("q", 0, 0));
        channel.Setup(c => c.QueueDeclareAsync(
                "",
                false,
                true,
                true,
                null,
                false,
                false,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new QueueDeclareOk("amq.gen-reply", 0, 0));
        channel.Setup(c => c.QueueBindAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<IDictionary<string, object?>>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        Mock<IConnection> connection = ConnectionWithChannel(channel);
        var builder = new BrokerOptionsBuilder { SetupName = "svc" };
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest",
            DeadLetterExchange = "dlx"
        });
        builder.AddRpcReceiver<TestMessage, TestReplyMessage>((TestMessage _) => Task.FromResult(new TestReplyMessage()));
        var resolver = new RabbitMQNameResolver(builder.SetupName);

        await TopologyDeclarer.EnsureRpcDeclared(connection.Object, resolver, builder, "dlx");

        channel.Verify(c => c.QueueDeclareAsync(
            "",
            false,
            true,
            true,
            null,
            false,
            false,
            It.IsAny<CancellationToken>()), Times.Once);
        channel.Verify(c => c.QueueDeclareAsync(
            "svc_TestMessage",
            false,
            false,
            false,
            It.Is<IDictionary<string, object?>>(args =>
                args != null
                && args.ContainsKey("x-dead-letter-exchange")
                && Equals(args["x-dead-letter-exchange"], "dlx")),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
        channel.Verify(c => c.ExchangeDeclareAsync(
            "dlx",
            ExchangeType.Fanout,
            true,
            false,
            It.IsAny<IDictionary<string, object?>>(),
            It.IsAny<bool>(),
            It.IsAny<bool>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task RabbitMq_SetupInfrastructure_registers_hosted_service()
    {
        var builder = new BrokerOptionsBuilder { SetupName = "svc" };
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        });
        var extension = (RabbitMQExtension)builder.Extension!;
        var services = new ServiceCollection();

        await extension.SetupInfrastructure(services, builder);

        services.Any(d => d.ServiceType == typeof(IHostedService)
            && (d.ImplementationType == typeof(RabbitMqBrokerHostedService)
                || d.ImplementationFactory is not null)).ShouldBeTrue();
    }

    [Fact]
    public void AddBroker_rabbitmq_produce_only_registers_hosted_service()
    {
        var services = new ServiceCollection();

        services.AddBroker(b =>
        {
            b.UseRabbitMq(new RabbitMQSettings
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            });
            b.AddProducerFor<TestMessage>();
        });

        services.Any(d => d.ServiceType == typeof(IHostedService)
            && (d.ImplementationType == typeof(RabbitMqBrokerHostedService)
                || d.ImplementationFactory is not null)).ShouldBeTrue();
    }

    [Fact]
    public async Task StartConsumers_twice_does_not_create_second_channel_when_open()
    {
        var channel = new Mock<IChannel>();
        SetupChannelLifecycle(channel);
        channel.SetupGet(c => c.IsOpen).Returns(true);
        channel.Setup(c => c.BasicQosAsync(
                It.IsAny<uint>(),
                It.IsAny<ushort>(),
                It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        channel.Setup(c => c.BasicConsumeAsync(
                It.IsAny<string>(),
                It.IsAny<bool>(),
                It.IsAny<IAsyncBasicConsumer>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync("tag");

        Mock<IConnection> connection = ConnectionWithChannel(channel);
        var builder = new BrokerOptionsBuilder { SetupName = "svc" };
        builder.UseRabbitMq(new RabbitMQSettings
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest",
            PrefetchCount = 10
        });
        builder.AddConsumerFor<TestMessage>((TestMessage _) => Task.FromResult(true));
        var extension = (RabbitMQExtension)builder.Extension!;

        var services = new ServiceCollection();
        services.AddSingleton<ILogger<RabbitMQBasicConsumer>>(NullLogger<RabbitMQBasicConsumer>.Instance);
        services.AddKeyedSingleton<INameResolver>(builder.BrokerId, new RabbitMQNameResolver(builder.SetupName));
        var runtime = new RabbitMqRuntime { Connection = connection.Object };
        services.AddKeyedSingleton(builder.BrokerId, runtime);
        await extension.SetupConsumers(services, builder);
        ServiceProvider provider = services.BuildServiceProvider();

        await extension.StartConsumers(provider, builder);
        await extension.StartConsumers(provider, builder);

        connection.Verify(
            c => c.CreateChannelAsync(It.IsAny<CreateChannelOptions>(), It.IsAny<CancellationToken>()),
            Times.Once);
        channel.Verify(
            c => c.BasicQosAsync(0, 10, false, It.IsAny<CancellationToken>()),
            Times.Once);
    }
}
