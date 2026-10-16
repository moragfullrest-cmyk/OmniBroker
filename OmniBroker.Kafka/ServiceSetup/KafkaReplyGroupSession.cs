using Confluent.Kafka;

namespace OmniBroker.Kafka.ServiceSetup;

internal sealed class KafkaReplyGroupSessionFactory(KafkaSettings settings, string topic) : IKafkaReplyGroupSessionFactory
{
    public IKafkaReplyGroupSession Open(string groupId) => new KafkaReplyGroupSession(settings, groupId, topic);
}

internal sealed class KafkaReplyGroupSession : IKafkaReplyGroupSession
{
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly string _groupId;
    private string _memberId = "";
    private int _disposed;

    public KafkaReplyGroupSession(KafkaSettings settings, string groupId, string topic)
    {
        _groupId = groupId;
        var config = new ConsumerConfig
        {
            BootstrapServers = settings.Hosts,
            GroupId = groupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        if (settings.ClientId is not null)
            config.ClientId = $"{settings.ClientId}.rpc-reply";
        KafkaClientSecurity.Apply(settings, config);
        _consumer = new ConsumerBuilder<string, byte[]>(config).Build();
        Topic = topic;
    }

    private string Topic { get; }

    public string MemberId => _memberId;

    public Task JoinAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(Topic);
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"Kafka RPC reply group '{_groupId}' was not joined within {timeout.TotalSeconds:0} seconds.");
            }

            _consumer.Consume(TimeSpan.FromMilliseconds(100));
            if (ReadMemberId() is { Length: > 0 } memberId)
            {
                _memberId = memberId;
                PauseCurrentAssignment();
                return Task.CompletedTask;
            }
        }
    }

    public void Poll(TimeSpan timeout)
    {
        _ = _consumer.Consume(timeout);
        PauseCurrentAssignment();
    }

    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return ValueTask.CompletedTask;

        try
        {
            _consumer.Close();
        }
        catch (KafkaException)
        {
        }
        catch (ObjectDisposedException)
        {
        }

        _consumer.Dispose();
        return ValueTask.CompletedTask;
    }

    private void PauseCurrentAssignment()
    {
        // Assignment is visible only after Consume returns. Pausing earlier is rejected for this partition.
        var assignment = _consumer.Assignment;
        if (assignment.Count > 0)
            _consumer.Pause(assignment);
    }

    private string? ReadMemberId()
    {
        string? memberId = _consumer.MemberId;
        return string.IsNullOrEmpty(memberId) ? null : memberId;
    }
}
