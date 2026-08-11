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
}
