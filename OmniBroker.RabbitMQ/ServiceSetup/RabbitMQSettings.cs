namespace OmniBroker.RabbitMQ.ServiceSetup;

/// <summary>
/// RabbitMQ connection settings
/// </summary>
public sealed class RabbitMQSettings
{
    /// <summary>
    /// Broker host
    /// </summary>
    public required string HostName { get; init; }

    /// <summary>
    /// User name
    /// </summary>
    public required string UserName { get; init; }

    /// <summary>
    /// Password
    /// </summary>
    public required string Password { get; init; }

    /// <summary>
    /// Virtual host
    /// </summary>
    public string VirtualHost { get; init; } = "/";

    /// <summary>
    /// Connection port
    /// </summary>
    public int Port { get; init; } = 5672;

    /// <summary>
    /// Use TLS
    /// </summary>
    public bool UseTls { get; init; }

    /// <summary>
    /// RPC response wait timeout
    /// </summary>
    public TimeSpan RpcTimeout { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Maximum channel pool size
    /// </summary>
    public int MaxChannelPoolSize { get; init; } = 32;

    /// <summary>
    /// Consumer prefetch count (basic.qos, per-consumer). Default is 1.
    /// </summary>
    public ushort PrefetchCount { get; init; } = 1;

    /// <summary>
    /// Optional dead-letter exchange. When set, consumer and RPC receive queues use x-dead-letter-exchange; handler failures are nacked without requeue.
    /// </summary>
    public string? DeadLetterExchange { get; init; }

    /// <summary>
    /// When true, consumable and RPC receive queues are declared durable. Default is true.
    /// Existing non-durable queues with the same name will fail with PRECONDITION_FAILED.
    /// Server-named exclusive auto-delete RPC reply queues are not durable.
    /// </summary>
    public bool DurableQueues { get; init; } = true;
}
