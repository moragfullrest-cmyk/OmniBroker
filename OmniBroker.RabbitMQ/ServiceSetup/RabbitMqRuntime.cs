using RabbitMQ.Client;

namespace OmniBroker.RabbitMQ.ServiceSetup;

internal sealed class RabbitMqRuntime
{
    public IConnection? Connection { get; set; }
    public ConcurrentObjectPool<IChannel>? ChannelPool { get; set; }
    public IChannel? ConsumerChannel { get; set; }
    public SemaphoreSlim ReconnectLock { get; } = new(1, 1);

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

    public async Task ResetConnectionAsync()
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
