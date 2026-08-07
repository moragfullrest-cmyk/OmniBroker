using RabbitMQ.Client;

namespace OmniBroker.RabbitMQ.ServiceSetup;

internal sealed class RabbitMqRuntime
{
    public IConnection? Connection { get; set; }
    public ConcurrentObjectPool<IChannel>? ChannelPool { get; set; }
    public SemaphoreSlim ReconnectLock { get; } = new(1, 1);

    public IConnection RequireConnection()
    {
        return Connection
            ?? throw new InvalidOperationException(
                "RabbitMQ connection is not started. Call UseBrokers / UseBrokersAsync first.");
    }

    public ConcurrentObjectPool<IChannel> RequireChannelPool()
    {
        return ChannelPool
            ?? throw new InvalidOperationException(
                "RabbitMQ channel pool is not started. Call UseBrokers / UseBrokersAsync first.");
    }

    public async Task ResetConnectionAsync()
    {
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

        if (ChannelPool is not null)
        {
            await ChannelPool.DisposeAsync();
        }

        ChannelPool = null;
    }
}
