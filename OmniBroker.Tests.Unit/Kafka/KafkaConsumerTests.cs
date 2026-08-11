using System.Reflection;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.Implementations;
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

    private static (KafkaConsumer Consumer, Mock<IConsumer<string, byte[]>> Kafka, List<ConsumeResult<string, byte[]>> Committed)
        Create(HandlerWrapper? handler = null, Action<Mock<IConsumer<string, byte[]>>>? configure = null)
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

        INameResolver resolver = new KafkaNameResolver();
        configure?.Invoke(kafka);

        var consumer = new KafkaConsumer(
            services.BuildServiceProvider(),
            brokerId,
            NullLogger<KafkaConsumer>.Instance,
            resolver);

        return (consumer, kafka, committed);
    }

    private static ConsumeResult<string, byte[]> Result(string topic, byte[]? body)
        => new()
        {
            Topic = topic,
            Message = new Message<string, byte[]> { Value = body! }
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
            });

        await RunExecuteAsync(consumer, cts.Token);

        committed.Count.ShouldBe(1);
    }

    [Fact]
    public async Task Handler_exception_commits_and_continues()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
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
            });

        await RunExecuteAsync(consumer, cts.Token);

        committed.Count.ShouldBe(1);
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
}
