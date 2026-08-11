using Microsoft.Extensions.DependencyInjection;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Core;

public sealed class MultiBrokerProducerTests
{
    private static MultiBrokerProducer<TestMessage> Create(params IProducer<TestMessage>[] producers)
    {
        var brokerIds = producers.Select(_ => new BrokerId()).ToArray();
        var builders = brokerIds.Select(id =>
        {
            var builder = new BrokerOptionsBuilder();
            builder.BrokerId = id;
            return builder;
        }).ToArray();

        var services = new ServiceCollection();
        foreach (BrokerOptionsBuilder builder in builders)
            services.AddSingleton(builder);

        for (int i = 0; i < producers.Length; i++)
        {
            IProducer<TestMessage> producer = producers[i];
            BrokerId id = brokerIds[i];
            services.AddKeyedSingleton(id, producer);
        }

        return new MultiBrokerProducer<TestMessage>(services.BuildServiceProvider());
    }

    [Fact]
    public async Task Publish_no_producers_returns_false()
    {
        MultiBrokerProducer<TestMessage> sut = Create();

        bool result = await sut.Publish(new TestMessage());

        result.ShouldBeFalse();
    }

    [Fact]
    public async Task Publish_single_producer_success()
    {
        var mock = new Mock<IProducer<TestMessage>>();
        mock.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        MultiBrokerProducer<TestMessage> sut = Create(mock.Object);

        bool result = await sut.Publish(new TestMessage());

        result.ShouldBeTrue();
        mock.Verify(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Publish_all_producers_true_returns_true()
    {
        var first = new Mock<IProducer<TestMessage>>();
        first.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var second = new Mock<IProducer<TestMessage>>();
        second.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        MultiBrokerProducer<TestMessage> sut = Create(first.Object, second.Object);

        bool result = await sut.Publish(new TestMessage());

        result.ShouldBeTrue();
    }

    [Fact]
    public async Task Publish_one_producer_false_returns_false()
    {
        var first = new Mock<IProducer<TestMessage>>();
        first.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var second = new Mock<IProducer<TestMessage>>();
        second.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        MultiBrokerProducer<TestMessage> sut = Create(first.Object, second.Object);

        bool result = await sut.Publish(new TestMessage());

        result.ShouldBeFalse();
    }

    [Fact]
    public async Task Publish_destination_with_multiple_producers_throws()
    {
        var first = new Mock<IProducer<TestMessage>>();
        var second = new Mock<IProducer<TestMessage>>();
        MultiBrokerProducer<TestMessage> sut = Create(first.Object, second.Object);

        await Should.ThrowAsync<InvalidOperationException>(() =>
            sut.Publish(new TestMessage(), new PublishOptions(null, null, "dest")));
    }

    [Fact]
    public async Task Publish_destination_with_single_producer_forwards_options()
    {
        var mock = new Mock<IProducer<TestMessage>>();
        mock.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        MultiBrokerProducer<TestMessage> sut = Create(mock.Object);
        var options = new PublishOptions(null, null, "custom-dest");

        bool result = await sut.Publish(new TestMessage(), options);

        result.ShouldBeTrue();
        mock.Verify(p => p.Publish(
            It.IsAny<TestMessage>(),
            It.Is<PublishOptions?>(o => o != null && o.Destination == "custom-dest"),
            It.IsAny<CancellationToken>()), Times.Once);
    }
}
