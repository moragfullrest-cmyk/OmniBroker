using System.Reflection;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using OmniBroker.Kafka.ServiceSetup;
using Shouldly;

namespace OmniBroker.Tests.Unit.Kafka;

public sealed class KafkaRpcReplyLeaseHostedServiceTests
{
    [Fact]
    public async Task Reply_loop_polls_claim_session_once_with_zero_timeout()
    {
        using var cts = new CancellationTokenSource();
        int calls = 0;
        var session = new RecordingSession();
        var replies = new Mock<IConsumer<string, byte[]>>();
        replies.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref calls) <= 3)
                {
                    return new ConsumeResult<string, byte[]>
                    {
                        Message = new Message<string, byte[]> { Value = [1] }
                    };
                }

                cts.Cancel();
                throw new OperationCanceledException(cts.Token);
            });

        var sut = new KafkaRpcReplyLeaseHostedService(
            new ServiceCollection().BuildServiceProvider(),
            "kafka",
            new KafkaSettings { Hosts = "localhost:9092" })
        {
            ClaimMembershipPollInterval = TimeSpan.FromHours(1)
        };
        SetField(sut, "_session", session);
        SetField(sut, "_replies", replies.Object);

        await RunExecuteAsync(sut, cts.Token);

        session.Polls.ShouldBe([TimeSpan.Zero]);
        replies.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, byte[]>>()), Times.Exactly(3));
        replies.Verify(c => c.Consume(TimeSpan.FromMilliseconds(100)), Times.Exactly(4));
    }

    private static async Task RunExecuteAsync(KafkaRpcReplyLeaseHostedService sut, CancellationToken token)
    {
        MethodInfo method = typeof(KafkaRpcReplyLeaseHostedService)
            .GetMethod("ExecuteAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await (Task)method.Invoke(sut, [token])!;
    }

    private static void SetField(object target, string name, object value)
    {
        typeof(KafkaRpcReplyLeaseHostedService)
            .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(target, value);
    }

    private sealed class RecordingSession : IKafkaReplyGroupSession
    {
        public List<TimeSpan> Polls { get; } = [];

        public string MemberId => "member";

        public Task JoinAsync(TimeSpan timeout, CancellationToken cancellationToken) => Task.CompletedTask;

        public void Poll(TimeSpan timeout) => Polls.Add(timeout);

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
