using RabbitMQ.Client;

namespace OmniBroker.RabbitMQ.ServiceSetup;

internal sealed class RabbitMqRuntime
{
    private readonly CancellationTokenSource _stopping = new();

    public IConnection? Connection { get; set; }
    public ConcurrentObjectPool<IChannel>? ChannelPool { get; set; }
    public IChannel? ConsumerChannel { get; set; }
    public SemaphoreSlim ReconnectLock { get; } = new(1, 1);

    /// <summary>
    /// Cancelled when the broker hosted service stops. Recover observes this token.
    /// </summary>
    public CancellationToken StoppingToken => _stopping.Token;

    /// <summary>
    /// Signal that the host is stopping so an in-flight recover releases <see cref="ReconnectLock"/>.
    /// </summary>
    public void SignalStop() => _stopping.Cancel();

    /// <summary>
    /// When true, consumer channel shutdown callbacks must not trigger recover
    /// (intentional close during reset).
    /// </summary>
    public bool SuppressConsumerShutdownRecover { get; set; }

    public IConnection RequireConnection()
    {
        return Connection
            ?? throw new InvalidOperationException(
                "RabbitMQ connection is not started. The host must be started so the broker hosted service can run.");
    }

    public ConcurrentObjectPool<IChannel> RequireChannelPool()
    {
        return ChannelPool
            ?? throw new InvalidOperationException(
                "RabbitMQ channel pool is not started. The host must be started so the broker hosted service can run.");
    }

    public async Task ResetConnectionAsync(CancellationToken cancellationToken = default)
    {
        await ReconnectLock.WaitAsync(cancellationToken);
        try
        {
            await ResetConnectionCoreAsync();
        }
        finally
        {
            ReconnectLock.Release();
        }
    }

    internal async Task ResetConnectionCoreAsync()
    {
        SuppressConsumerShutdownRecover = true;

        if (ConsumerChannel is not null)
        {
            try
            {
                if (ConsumerChannel.IsOpen)
                    await ConsumerChannel.CloseAsync();
            }
            catch
            {
                // ignore
            }

            try
            {
                await ConsumerChannel.DisposeAsync();
            }
            catch
            {
                // ignore
            }
        }

        ConsumerChannel = null;

        if (ChannelPool is not null)
        {
            await ChannelPool.ClearAsync();
        }

        if (Connection is not null)
        {
            try
            {
                await Connection.CloseAsync();
            }
            catch
            {
                // ignore
            }

            try
            {
                Connection.Dispose();
            }
            catch
            {
                // ignore
            }
        }

        Connection = null;
    }
}
