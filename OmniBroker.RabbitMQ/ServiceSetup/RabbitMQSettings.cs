namespace OmniBroker.RabbitMQ.ServiceSetup;

public class RabbitMQSettings
{
    public required string HostName { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
    public string VirtualHost { get; init; } = "/";
    public int Port { get; init; } = 5672;
    public bool UseTls { get; init; }
    public TimeSpan RpcTimeout { get; init; } = TimeSpan.FromMinutes(1);
    public int MaxChannelPoolSize { get; init; } = 32;
}
