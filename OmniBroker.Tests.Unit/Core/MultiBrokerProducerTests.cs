using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Core;

public sealed class MultiBrokerProducerTests
{
    private static MultiBrokerProducer<TestMessage> Create(params IProducer<TestMessage>[] producers)
        => CreateWithIds(producers).Sut;

    private static (MultiBrokerProducer<TestMessage> Sut, string[] BrokerIds) CreateWithIds(params IProducer<TestMessage>[] producers)
    {
        var brokerIds = producers.Select(_ => Guid.NewGuid().ToString()).ToArray();
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
            string id = brokerIds[i];
            services.AddKeyedSingleton(id, producer);
        }

        return (new MultiBrokerProducer<TestMessage>(services.BuildServiceProvider()), brokerIds);
    }

    [Fact]
    public async Task Publish_no_producers_throws()
    {
        MultiBrokerProducer<TestMessage> sut = Create();

        InvalidOperationException ex = await Should.ThrowAsync<InvalidOperationException>(() => sut.Publish(new TestMessage()));

        ex.Message.ShouldContain(nameof(TestMessage));
        ex.Message.ShouldContain("No producers");
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
    public async Task Publish_single_producer_false_returns_false()
    {
        var mock = new Mock<IProducer<TestMessage>>();
        mock.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        MultiBrokerProducer<TestMessage> sut = Create(mock.Object);

        bool result = await sut.Publish(new TestMessage());

        result.ShouldBeFalse();
    }

    [Fact]
    public async Task Publish_partial_failure_throws_with_per_broker_outcomes()
    {
        var first = new Mock<IProducer<TestMessage>>();
        first.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var second = new Mock<IProducer<TestMessage>>();
        second.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        (MultiBrokerProducer<TestMessage> sut, string[] brokerIds) = CreateWithIds(first.Object, second.Object);

        MultiBrokerPublishException ex = await Should.ThrowAsync<MultiBrokerPublishException>(() => sut.Publish(new TestMessage()));

        ex.MessageType.ShouldBe(typeof(TestMessage));
        ex.Message.ShouldContain(brokerIds[1]);
        ex.Message.ShouldContain(brokerIds[0]);
        ex.Outcomes.Count.ShouldBe(2);
        ex.Outcomes[0].ShouldBe(new BrokerPublishOutcome(brokerIds[0], first.Object.GetType().Name, true));
        ex.Outcomes[1].ShouldBe(new BrokerPublishOutcome(brokerIds[1], second.Object.GetType().Name, false));
        first.Verify(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()), Times.Once);
        second.Verify(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Publish_one_producer_false_logs_warning()
    {
        var first = new Mock<IProducer<TestMessage>>();
        first.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var second = new Mock<IProducer<TestMessage>>();
        second.Setup(p => p.Publish(It.IsAny<TestMessage>(), It.IsAny<PublishOptions?>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        var logger = new Mock<ILogger<MultiBrokerProducer<TestMessage>>>();

        var firstId = Guid.NewGuid().ToString();
        var secondId = Guid.NewGuid().ToString();
        var firstBuilder = new BrokerOptionsBuilder { BrokerId = firstId };
        var secondBuilder = new BrokerOptionsBuilder { BrokerId = secondId };

        var services = new ServiceCollection();
        services.AddSingleton(firstBuilder);
        services.AddSingleton(secondBuilder);
        services.AddKeyedSingleton(firstId, first.Object);
        services.AddKeyedSingleton(secondId, second.Object);
        services.AddSingleton<ILogger<MultiBrokerProducer<TestMessage>>>(logger.Object);

        var sut = new MultiBrokerProducer<TestMessage>(services.BuildServiceProvider());

        MultiBrokerPublishException ex = await Should.ThrowAsync<MultiBrokerPublishException>(() => sut.Publish(new TestMessage()));

        ex.Outcomes.Single(outcome => outcome.BrokerId == secondId).Succeeded.ShouldBeFalse();
        logger.Verify(
            x => x.Log(
                LogLevel.Warning,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((state, _) =>
                    state.ToString()!.Contains(second.Object.GetType().Name)
                    && state.ToString()!.Contains(nameof(TestMessage))),
                It.IsAny<Exception?>(),
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
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
