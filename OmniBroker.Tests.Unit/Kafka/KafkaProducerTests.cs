using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.Implementations;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Kafka;

public sealed class KafkaProducerTests
{
    private static (KafkaProducer<TestMessage> Sut, Mock<IProducer<string, byte[]>> Producer, Mock<INameResolver> Resolver)
        CreateSut(Action<Mock<IProducer<string, byte[]>>>? configure = null)
    {
        var producer = new Mock<IProducer<string, byte[]>>();
        producer.Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, byte[]>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, byte[]> { Status = PersistenceStatus.Persisted });
        configure?.Invoke(producer);

        var resolver = new Mock<INameResolver>();
        resolver.Setup(r => r.ResolveOutboundName(typeof(TestMessage))).Returns(nameof(TestMessage));

        var sut = new KafkaProducer<TestMessage>(
            NullLogger<KafkaProducer<TestMessage>>.Instance,
            producer.Object,
            resolver.Object);

        return (sut, producer, resolver);
    }

    [Fact]
    public async Task Publish_null_message_throws()
    {
        (KafkaProducer<TestMessage> sut, _, _) = CreateSut();

        await Should.ThrowAsync<ArgumentNullException>(() => sut.Publish(null!));
    }

    [Fact]
    public async Task Publish_success_sets_correlation_header()
    {
        Message<string, byte[]>? captured = null;
        (KafkaProducer<TestMessage> sut, Mock<IProducer<string, byte[]>> producer, _) = CreateSut(p =>
        {
            p.Setup(x => x.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
                .Callback<string, Message<string, byte[]>, CancellationToken>((_, m, _) => captured = m)
                .ReturnsAsync(new DeliveryResult<string, byte[]> { Status = PersistenceStatus.Persisted });
        });

        bool result = await sut.Publish(
            new TestMessage { Body = [1], Tag = "key", CorrelationId = "from-message" },
            new PublishOptions("corr-1", null, null));

        result.ShouldBeTrue();
        captured.ShouldNotBeNull();
        captured!.Headers.ShouldNotBeNull();
        Encoding.UTF8.GetString(captured.Headers!.GetLastBytes(KafkaMessageHeaders.CorrelationId))
            .ShouldBe("corr-1");
        producer.Verify(p => p.ProduceAsync(nameof(TestMessage), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Publish_without_correlation_omits_headers()
    {
        Message<string, byte[]>? captured = null;
        (KafkaProducer<TestMessage> sut, _, _) = CreateSut(p =>
        {
            p.Setup(x => x.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
                .Callback<string, Message<string, byte[]>, CancellationToken>((_, m, _) => captured = m)
                .ReturnsAsync(new DeliveryResult<string, byte[]> { Status = PersistenceStatus.Persisted });
        });

        await sut.Publish(new TestMessage { Body = [1], Tag = "key", CorrelationId = "" });

        captured.ShouldNotBeNull();
        captured!.Headers.ShouldBeNull();
    }

    [Fact]
    public async Task Publish_destination_overrides_topic()
    {
        (KafkaProducer<TestMessage> sut, Mock<IProducer<string, byte[]>> producer, _) = CreateSut();

        await sut.Publish(new TestMessage { Body = [1] }, new PublishOptions(null, null, "custom-topic"));

        producer.Verify(p => p.ProduceAsync("custom-topic", It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Publish_uses_tag_as_key()
    {
        Message<string, byte[]>? captured = null;
        (KafkaProducer<TestMessage> sut, _, _) = CreateSut(p =>
        {
            p.Setup(x => x.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
                .Callback<string, Message<string, byte[]>, CancellationToken>((_, m, _) => captured = m)
                .ReturnsAsync(new DeliveryResult<string, byte[]> { Status = PersistenceStatus.Persisted });
        });

        await sut.Publish(new TestMessage { Body = [1], Tag = "partition-key" });

        captured!.Key.ShouldBe("partition-key");
    }

    [Fact]
    public async Task Publish_ProduceException_returns_false()
    {
        (KafkaProducer<TestMessage> sut, _, _) = CreateSut(p =>
        {
            p.Setup(x => x.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new ProduceException<string, byte[]>(
                    new Error(ErrorCode.Local_Transport, "fail"),
                    new DeliveryResult<string, byte[]>()));
        });

        bool result = await sut.Publish(new TestMessage { Body = [1] });

        result.ShouldBeFalse();
    }

    [Fact]
    public async Task Publish_NotPersisted_returns_false()
    {
        (KafkaProducer<TestMessage> sut, _, _) = CreateSut(p =>
        {
            p.Setup(x => x.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeliveryResult<string, byte[]>
                {
                    Status = PersistenceStatus.NotPersisted,
                    Topic = "t"
                });
        });

        bool result = await sut.Publish(new TestMessage { Body = [1] });

        result.ShouldBeFalse();
    }
}
