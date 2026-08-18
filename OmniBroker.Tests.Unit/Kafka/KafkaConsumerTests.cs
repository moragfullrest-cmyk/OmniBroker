using System.Reflection;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.Implementations;
using OmniBroker.Kafka.ServiceSetup;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Kafka;

public sealed class KafkaConsumerTests
{
    private static async Task RunExecuteAsync(KafkaConsumer consumer, CancellationToken token)
    {
        MethodInfo method = typeof(KafkaConsumer)
            .GetMethod("ExecuteAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task)method.Invoke(consumer, [token])!;
    }

    private static KafkaSettings Settings(string? deadLetterTopic = null) => new()
    {
        Hosts = "localhost:9092",
        DeadLetterTopic = deadLetterTopic
    };

    private static (KafkaConsumer Consumer, Mock<IConsumer<string, byte[]>> Kafka, List<ConsumeResult<string, byte[]>> Committed)
        Create(
            HandlerWrapper? handler = null,
            Action<Mock<IConsumer<string, byte[]>>>? configure = null,
            Mock<IProducer<string, byte[]>>? producer = null,
            KafkaSettings? settings = null)
    {
        var brokerId = new BrokerId();
        var kafka = new Mock<IConsumer<string, byte[]>>();
        var committed = new List<ConsumeResult<string, byte[]>>();
        kafka.Setup(c => c.Commit(It.IsAny<ConsumeResult<string, byte[]>>()))
            .Callback<ConsumeResult<string, byte[]>>(r => committed.Add(r));

        var services = new ServiceCollection();
        services.AddKeyedSingleton(brokerId, kafka.Object);
        if (handler is not null)
            services.AddKeyedSingleton(brokerId, handler);
        if (producer is not null)
            services.AddKeyedSingleton(brokerId, producer.Object);

        INameResolver resolver = new KafkaNameResolver();
        configure?.Invoke(kafka);

        var consumer = new KafkaConsumer(
            services.BuildServiceProvider(),
            brokerId,
            NullLogger<KafkaConsumer>.Instance,
            resolver,
            settings);

        return (consumer, kafka, committed);
    }

    private static ConsumeResult<string, byte[]> Result(string topic, byte[]? body, string? key = null, Headers? headers = null)
        => new()
        {
            Topic = topic,
            Message = new Message<string, byte[]> { Key = key!, Value = body!, Headers = headers }
        };

    [Fact]
    public async Task Empty_body_commits_without_handlers()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
        bool handlerCalled = false;
        (KafkaConsumer consumer, Mock<IConsumer<string, byte[]>> kafka, List<ConsumeResult<string, byte[]>> committed) = Create(
            new HandlerWrapper(typeof(TestMessage),
            [
                (_, _, _) =>
                {
                    handlerCalled = true;
                    return Task.FromResult(true);
                }
            ], HandlerWrapper.BuildCreateMessage(typeof(TestMessage))),
            c =>
            {
                c.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        if (Interlocked.Increment(ref calls) == 1)
                            return Result(nameof(TestMessage), []);
                        cts.Cancel();
                        throw new OperationCanceledException(cts.Token);
                    });
            });

        await RunExecuteAsync(consumer, cts.Token);

        handlerCalled.ShouldBeFalse();
        committed.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Unknown_topic_commits_and_skips()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
        (KafkaConsumer consumer, _, List<ConsumeResult<string, byte[]>> committed) = Create(
            configure: c =>
            {
                c.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        if (Interlocked.Increment(ref calls) == 1)
                            return Result("UnknownTopic", [1]);
                        cts.Cancel();
                        throw new OperationCanceledException(cts.Token);
                    });
            });

        await RunExecuteAsync(consumer, cts.Token);

        committed.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Handler_false_still_commits()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
        var producer = new Mock<IProducer<string, byte[]>>();
        (KafkaConsumer consumer, _, List<ConsumeResult<string, byte[]>> committed) = Create(
            new HandlerWrapper(typeof(TestMessage),
            [
                (_, _, _) => Task.FromResult(false)
            ], HandlerWrapper.BuildCreateMessage(typeof(TestMessage))),
            c =>
            {
                c.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        if (Interlocked.Increment(ref calls) == 1)
                            return Result(nameof(TestMessage), [1]);
                        cts.Cancel();
                        throw new OperationCanceledException(cts.Token);
                    });
            },
            producer);

        await RunExecuteAsync(consumer, cts.Token);

        committed.Count.ShouldBe(1);
        producer.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Handler_exception_commits_and_continues()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
        var producer = new Mock<IProducer<string, byte[]>>();
        (KafkaConsumer consumer, _, List<ConsumeResult<string, byte[]>> committed) = Create(
            new HandlerWrapper(typeof(TestMessage),
            [
                (_, _, _) => throw new InvalidOperationException("boom")
            ], HandlerWrapper.BuildCreateMessage(typeof(TestMessage))),
            c =>
            {
                c.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        if (Interlocked.Increment(ref calls) == 1)
                            return Result(nameof(TestMessage), [1]);
                        cts.Cancel();
                        throw new OperationCanceledException(cts.Token);
                    });
            },
            producer);

        await RunExecuteAsync(consumer, cts.Token);

        committed.Count.ShouldBe(1);
        producer.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task Cancel_token_exits_loop()
    {
        using var cts = new CancellationTokenSource();
        (KafkaConsumer consumer, Mock<IConsumer<string, byte[]>> kafka, _) = Create(
            configure: c =>
            {
                c.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
                    .Returns((CancellationToken token) =>
                    {
                        token.ThrowIfCancellationRequested();
                        cts.Cancel();
                        token.ThrowIfCancellationRequested();
                        return Result("t", [1]);
                    });
            });

        await RunExecuteAsync(consumer, cts.Token);

        kafka.Verify(c => c.Consume(It.IsAny<CancellationToken>()), Times.AtLeastOnce);
    }

    [Fact]
    public async Task ConsumeException_logged_and_loop_continues()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
        (KafkaConsumer consumer, _, List<ConsumeResult<string, byte[]>> committed) = Create(
            new HandlerWrapper(typeof(TestMessage),
            [
                (_, _, _) => Task.FromResult(true)
            ], HandlerWrapper.BuildCreateMessage(typeof(TestMessage))),
            c =>
            {
                c.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        int n = Interlocked.Increment(ref calls);
                        if (n == 1)
                            throw new ConsumeException(
                                new ConsumeResult<byte[], byte[]>(),
                                new Error(ErrorCode.Local_Transport, "fail"));
                        if (n == 2)
                            return Result(nameof(TestMessage), [1]);
                        cts.Cancel();
                        throw new OperationCanceledException(cts.Token);
                    });
            });

        await RunExecuteAsync(consumer, cts.Token);

        committed.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Handler_false_with_dead_letter_topic_produces_then_commits()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
        byte[] body = [1, 2];
        var producer = new Mock<IProducer<string, byte[]>>();
        producer.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, byte[]>());

        (KafkaConsumer consumer, _, List<ConsumeResult<string, byte[]>> committed) = Create(
            new HandlerWrapper(typeof(TestMessage),
            [
                (_, _, _) => Task.FromResult(false)
            ], HandlerWrapper.BuildCreateMessage(typeof(TestMessage))),
            c =>
            {
                c.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        if (Interlocked.Increment(ref calls) == 1)
                            return Result(nameof(TestMessage), body, key: "k1");
                        cts.Cancel();
                        throw new OperationCanceledException(cts.Token);
                    });
            },
            producer,
            Settings("dlq-topic"));

        await RunExecuteAsync(consumer, cts.Token);

        producer.Verify(p => p.ProduceAsync(
            "dlq-topic",
            It.Is<Message<string, byte[]>>(m =>
                m.Key == "k1"
                && m.Value == body
                && m.Headers.Any(h => h.Key == KafkaMessageHeaders.SourceTopic
                    && Encoding.UTF8.GetString(h.GetValueBytes()) == nameof(TestMessage))),
            It.IsAny<CancellationToken>()), Times.Once);
        committed.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Handler_false_with_dead_letter_topic_produce_throws_still_commits()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
        var producer = new Mock<IProducer<string, byte[]>>();
        producer.Setup(p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("produce failed"));

        (KafkaConsumer consumer, _, List<ConsumeResult<string, byte[]>> committed) = Create(
            new HandlerWrapper(typeof(TestMessage),
            [
                (_, _, _) => Task.FromResult(false)
            ], HandlerWrapper.BuildCreateMessage(typeof(TestMessage))),
            c =>
            {
                c.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        if (Interlocked.Increment(ref calls) == 1)
                            return Result(nameof(TestMessage), [1]);
                        cts.Cancel();
                        throw new OperationCanceledException(cts.Token);
                    });
            },
            producer,
            Settings("dlq-topic"));

        await RunExecuteAsync(consumer, cts.Token);

        committed.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Unknown_topic_with_dead_letter_topic_commits_without_produce()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
        var producer = new Mock<IProducer<string, byte[]>>();
        (KafkaConsumer consumer, _, List<ConsumeResult<string, byte[]>> committed) = Create(
            configure: c =>
            {
                c.Setup(x => x.Consume(It.IsAny<CancellationToken>()))
                    .Returns(() =>
                    {
                        if (Interlocked.Increment(ref calls) == 1)
                            return Result("UnknownTopic", [1]);
                        cts.Cancel();
                        throw new OperationCanceledException(cts.Token);
                    });
            },
            producer: producer,
            settings: Settings("dlq-topic"));

        await RunExecuteAsync(consumer, cts.Token);

        committed.Count.ShouldBe(1);
        producer.Verify(
            p => p.ProduceAsync(It.IsAny<string>(), It.IsAny<Message<string, byte[]>>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }
}
