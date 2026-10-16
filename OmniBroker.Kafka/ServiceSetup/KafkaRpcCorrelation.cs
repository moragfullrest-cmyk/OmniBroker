using System.Collections.Concurrent;
using OmniBroker;

namespace OmniBroker.Kafka.ServiceSetup;

internal sealed class KafkaRpcWait(TaskCompletionSource<IMessage> completion, Func<IMessage> create)
{
    public TaskCompletionSource<IMessage> Completion { get; } = completion;

    public Func<IMessage> Create { get; } = create;
}

internal static class KafkaRpcCorrelation
{
    internal static bool TryComplete(
        ConcurrentDictionary<string, KafkaRpcWait> pending,
        string? correlationId,
        byte[] body)
    {
        if (string.IsNullOrEmpty(correlationId))
            return false;

        if (!pending.TryRemove(correlationId, out KafkaRpcWait? wait))
            return false;

        IMessage message = wait.Create();
        message.Body = body;
        message.CorrelationId = correlationId;
        wait.Completion.TrySetResult(message);
        return true;
    }

    internal static void CancelPending(ConcurrentDictionary<string, KafkaRpcWait> pending)
    {
        foreach (KeyValuePair<string, KafkaRpcWait> pair in pending)
        {
            if (pending.TryRemove(pair.Key, out KafkaRpcWait? wait))
                wait.Completion.TrySetCanceled();
        }
    }
}
