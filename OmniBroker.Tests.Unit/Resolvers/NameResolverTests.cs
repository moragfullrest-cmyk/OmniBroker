using OmniBroker.Kafka.Implementations;
using OmniBroker.RabbitMQ.Implementations;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Resolvers;

public sealed class NameResolverTests
{
    [Fact]
    public void RabbitMQ_inbound_uses_prefix()
    {
        var resolver = new RabbitMQNameResolver("svc");

        resolver.ResolveInboundName(typeof(TestMessage)).ShouldBe("svc_TestMessage");
    }

    [Fact]
    public void RabbitMQ_outbound_uses_type_name()
    {
        var resolver = new RabbitMQNameResolver("svc");

        resolver.ResolveOutboundName(typeof(TestMessage)).ShouldBe(nameof(TestMessage));
    }

    [Fact]
    public void RabbitMQ_non_message_type_throws()
    {
        var resolver = new RabbitMQNameResolver("svc");

        Should.Throw<ArgumentException>(() => resolver.ResolveInboundName(typeof(string)));
        Should.Throw<ArgumentException>(() => resolver.ResolveOutboundName(typeof(string)));
    }

    [Fact]
    public void Kafka_inbound_and_outbound_use_type_name()
    {
        var resolver = new KafkaNameResolver();

        resolver.ResolveInboundName(typeof(TestMessage)).ShouldBe(nameof(TestMessage));
        resolver.ResolveOutboundName(typeof(TestMessage)).ShouldBe(nameof(TestMessage));
    }

    [Fact]
    public void Kafka_non_message_type_throws()
    {
        var resolver = new KafkaNameResolver();

        Should.Throw<ArgumentException>(() => resolver.ResolveInboundName(typeof(int)));
        Should.Throw<ArgumentException>(() => resolver.ResolveOutboundName(typeof(int)));
    }
}
