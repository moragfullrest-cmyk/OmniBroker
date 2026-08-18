using Microsoft.Extensions.DependencyInjection;
using OmniBroker.Infrastructure;
using OmniBroker.Kafka.ServiceSetup;
using OmniBroker.RabbitMQ;
using OmniBroker.RabbitMQ.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Core;

public sealed class BrokerExtensionsTests
{
    private static RabbitMQSettings RabbitSettings() => new()
    {
        HostName = "localhost",
        UserName = "guest",
        Password = "guest"
    };

    private static KafkaSettings KafkaSettings() => new() { Hosts = "localhost:9092" };

    [Fact]
    public void AddBroker_null_action_throws()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentNullException>(() => services.AddBroker(null!));
    }

    [Fact]
    public void AddBroker_without_extension_throws()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentNullException>(() => services.AddBroker(_ => { }));
    }

    [Fact]
    public void AddBroker_rpc_on_kafka_throws()
    {
        var services = new ServiceCollection();

        Should.Throw<ArgumentException>(() => services.AddBroker(b =>
        {
            b.UseKafka(KafkaSettings());
            b.AddRpcCaller<TestMessage, TestReplyMessage>();
        }));
    }

    [Fact]
    public void AddProducerFor_duplicate_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());
        builder.AddProducerFor<TestMessage>();

        Should.Throw<ArgumentException>(() => builder.AddProducerFor<TestMessage>());
    }

    [Fact]
    public void AddProducerFor_when_type_in_rpc_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());
        builder.AddRpcCaller<TestMessage, TestReplyMessage>();

        Should.Throw<ArgumentException>(() => builder.AddProducerFor<TestMessage>());
    }

    [Fact]
    public void AddConsumerFor_null_delegate_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());

        Should.Throw<ArgumentNullException>(() => builder.AddConsumerFor<TestMessage>(null!));
    }

    [Fact]
    public void AddConsumerFor_wrong_return_type_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());

        Should.Throw<ArgumentException>(() => builder.AddConsumerFor<TestMessage>(
            (TestMessage _) => Task.CompletedTask));
        Should.Throw<ArgumentException>(() => builder.AddConsumerFor<TestMessage>(
            (TestMessage _) => Task.FromResult(true)));
    }

    [Fact]
    public async Task AddConsumerFor_CancellationToken_parameter_receives_token_from_MessageContext()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());
        using var cts = new CancellationTokenSource();
        CancellationToken captured = default;

        builder.AddConsumerFor<TestMessage>((TestMessage _, CancellationToken token) =>
        {
            captured = token;
            return Task.FromResult(HandleResult.Ack);
        });

        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> handler =
            builder.Consumables[typeof(TestMessage)].Single();
        HandleResult result = await handler(
            new ServiceCollection().BuildServiceProvider(),
            new TestMessage(),
            new MessageContext(builder.BrokerId, null, cts.Token));

        result.ShouldBe(HandleResult.Ack);
        captured.ShouldBe(cts.Token);
    }

    [Fact]
    public void AddConsumerFor_missing_message_parameter_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());

        Should.Throw<ArgumentException>(() => builder.AddConsumerFor<TestMessage>(
            () => Task.FromResult(HandleResult.Ack)));
    }

    [Fact]
    public void AddConsumerFor_abstract_message_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());

        Should.Throw<ArgumentException>(() => builder.AddConsumerFor<AbstractTestMessage>(
            (AbstractTestMessage _) => Task.FromResult(HandleResult.Ack)));
    }

    [Fact]
    public void AddRpcCaller_duplicate_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());
        builder.AddRpcCaller<TestMessage, TestReplyMessage>();

        Should.Throw<ArgumentException>(() => builder.AddRpcCaller<TestMessage, TestReplyMessage>());
    }

    [Fact]
    public void AddRpcCaller_conflicts_with_producer_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());
        builder.AddProducerFor<TestMessage>();

        Should.Throw<ArgumentException>(() => builder.AddRpcCaller<TestMessage, TestReplyMessage>());
    }

    [Fact]
    public void AddRpcReceiver_wrong_return_type_throws()
    {
        var builder = new BrokerOptionsBuilder();
        builder.UseRabbitMq(RabbitSettings());

        Should.Throw<ArgumentException>(() => builder.AddRpcReceiver<TestMessage, TestReplyMessage>(
            (TestMessage _) => Task.FromResult(true)));
    }

    [Fact]
    public void AddBroker_registers_producer_and_consumer()
    {
        var services = new ServiceCollection();

        services.AddBroker(b =>
        {
            b.UseRabbitMq(RabbitSettings());
            b.AddProducerFor<TestMessage>();
            b.AddConsumerFor<TestMessage>((TestMessage _) => Task.FromResult(HandleResult.Ack));
        });

        services.Any(d => d.ServiceType == typeof(IProducer<TestMessage>)).ShouldBeTrue();
        services.Any(d => d.ServiceType == typeof(BrokerOptionsBuilder)).ShouldBeTrue();
        services.Any(d => d.IsKeyedService && d.ServiceKey is BrokerId && d.ServiceType == typeof(HandlerWrapper))
            .ShouldBeTrue();
    }

    [Fact]
    public void AddBroker_two_brokers_registers_multi_and_keyed_producers()
    {
        var services = new ServiceCollection();

        services.AddBroker(b =>
        {
            b.UseRabbitMq(RabbitSettings());
            b.AddProducerFor<TestMessage>();
        });
        services.AddBroker(b =>
        {
            b.UseKafka(KafkaSettings());
            b.AddProducerFor<TestMessage>();
        });

        var unkeyed = services.Where(d => d.ServiceType == typeof(IProducer<TestMessage>) && !d.IsKeyedService).ToList();
        unkeyed.ShouldNotBeEmpty();
        unkeyed.ShouldAllBe(d => d.ImplementationType == typeof(MultiBrokerProducer<TestMessage>));

        services.Count(d => d.IsKeyedService && d.ServiceType == typeof(IProducer<TestMessage>)).ShouldBe(2);
        services.Count(d => d.ServiceType == typeof(BrokerOptionsBuilder) && !d.IsKeyedService).ShouldBe(2);
    }
}
