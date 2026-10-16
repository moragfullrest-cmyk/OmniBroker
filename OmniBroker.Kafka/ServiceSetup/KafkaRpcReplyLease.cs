namespace OmniBroker.Kafka.ServiceSetup;

internal sealed class KafkaRpcReplyLease
{
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int? PartitionId { get; private set; }

    public string? GroupId { get; private set; }

    public Task Ready => _ready.Task;

    internal void Claim(int partitionId, string groupId)
    {
        PartitionId = partitionId;
        GroupId = groupId;
    }

    internal void MarkReady() => _ready.TrySetResult();

    internal void Fail(Exception exception) => _ready.TrySetException(exception);

    internal void Cancel() => _ready.TrySetCanceled();
}
