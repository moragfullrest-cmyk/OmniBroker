using System.Collections.Concurrent;
using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using OmniBroker;
using OmniBroker.Kafka.Implementations;

namespace OmniBroker.Kafka.ServiceSetup;

internal sealed class KafkaRpcReplyLeaseHostedService(
    IServiceProvider services,
    string brokerId,
    KafkaSettings settings) : BackgroundService
{
    private readonly ILogger _logger = services.GetService<ILogger<KafkaRpcReplyLeaseHostedService>>()
        ?? NullLogger<KafkaRpcReplyLeaseHostedService>.Instance;

    private IKafkaReplyGroupSession? _session;
    private IConsumer<string, byte[]>? _replies;
    private ConcurrentDictionary<string, KafkaRpcWait>? _pending;
    private KafkaRpcReplyLease? _lease;
    private DateTime _nextClaimPollAt;

    /// <summary>
    /// How often the paused claim session is polled so the group membership stays alive.
    /// The poll itself does not wait: replies are read on the next line.
    /// </summary>
    internal TimeSpan ClaimMembershipPollInterval { get; set; } = TimeSpan.FromSeconds(5);

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        BrokerOptionsBuilder builder = services.GetRequiredKeyedService<BrokerOptionsBuilder>(brokerId);
        KafkaRpcReplyLease lease = services.GetRequiredKeyedService<KafkaRpcReplyLease>(brokerId);
        _lease = lease;
        try
        {
            _pending = services.GetRequiredKeyedService<ConcurrentDictionary<string, KafkaRpcWait>>(brokerId);
            using var metadata = new KafkaAdminMetadataClient(settings);
            var sessions = new KafkaReplyGroupSessionFactory(settings, settings.RpcReplyTopic ?? "");
            KafkaRpcReplyHold hold = await KafkaRpcReplyClaim.ClaimAsync(
                brokerId,
                settings,
                builder,
                metadata,
                sessions,
                KafkaAdminMetadataClient.Timeout,
                cancellationToken);
            lease.Claim(hold.PartitionId, hold.GroupId);
            _session = hold.Session;
            _replies = CreateReplyConsumer(hold.GroupId);
            AssignReplyStart(_replies, settings.RpcReplyTopic!, hold.PartitionId, lease);
            await base.StartAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            lease.Fail(ex);
            DisposeReplyConsumer();
            await DisposeSessionAsync();
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken);
        if (_pending is not null)
            KafkaRpcCorrelation.CancelPending(_pending);
        _lease?.Cancel();
        DisposeReplyConsumer();
        await DisposeSessionAsync();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        while (stoppingToken.IsCancellationRequested == false)
        {
            try
            {
                PollClaimMembershipIfDue();
                ConsumeResult<string, byte[]>? received = _replies?.Consume(TimeSpan.FromMilliseconds(100));
                if (received is null)
                    continue;

                byte[]? body = received.Message?.Value;
                if (body is not null && _pending is not null)
                {
                    KafkaRpcCorrelation.TryComplete(
                        _pending,
                        ReadCorrelationId(received.Message?.Headers),
                        body);
                }

                _replies?.Commit(received);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka RPC reply consume error: {Reason}", ex.Error.Reason);
                if (!await DelayAsync(stoppingToken))
                    break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error correlating a Kafka RPC reply");
                if (!await DelayAsync(stoppingToken))
                    break;
            }
        }
    }

    /// <summary>
    /// Freezes the reply reader at the current high watermark, then marks the lease ready.
    /// <see cref="Offset.End"/> would be resolved on the first consume, after callers are already allowed to publish.
    /// </summary>
    internal void AssignReplyStart(
        IConsumer<string, byte[]> replies,
        string topic,
        int partitionId,
        KafkaRpcReplyLease lease)
    {
        var partition = new TopicPartition(topic, new Partition(partitionId));
        WatermarkOffsets marks = replies.QueryWatermarkOffsets(partition, KafkaAdminMetadataClient.Timeout);
        replies.Assign(new TopicPartitionOffset(partition, marks.High));
        lease.MarkReady();
    }

    private void PollClaimMembershipIfDue()
    {
        DateTime now = DateTime.UtcNow;
        if (now < _nextClaimPollAt)
            return;

        _session?.Poll(TimeSpan.Zero);
        _nextClaimPollAt = now + ClaimMembershipPollInterval;
    }

    private IConsumer<string, byte[]> CreateReplyConsumer(string claimGroupId)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = settings.Hosts,
            // Not the claim group. The client still requires a group id so offset commits succeed.
            // Assign, not Subscribe, so this consumer does not take the locked partitions.
            GroupId = claimGroupId + ".read",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        if (settings.ClientId is not null)
            config.ClientId = $"{settings.ClientId}.rpc-reply-read";
        KafkaClientSecurity.Apply(settings, config);
        return new ConsumerBuilder<string, byte[]>(config).Build();
    }

    private static string? ReadCorrelationId(Headers? headers)
    {
        if (headers is null)
            return null;

        IHeader? header = headers.FirstOrDefault(item => item.Key == KafkaMessageHeaders.CorrelationId);
        if (header is null)
            return null;

        return Encoding.UTF8.GetString(header.GetValueBytes());
    }

    private static async Task<bool> DelayAsync(CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(200), stoppingToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private void DisposeReplyConsumer()
    {
        if (_replies is null)
            return;

        IConsumer<string, byte[]> replies = _replies;
        _replies = null;
        try
        {
            replies.Close();
        }
        catch (KafkaException)
        {
        }
        catch (ObjectDisposedException)
        {
        }

        replies.Dispose();
    }

    private async Task DisposeSessionAsync()
    {
        if (_session is null)
            return;

        IKafkaReplyGroupSession session = _session;
        _session = null;
        await session.DisposeAsync();
    }
}
