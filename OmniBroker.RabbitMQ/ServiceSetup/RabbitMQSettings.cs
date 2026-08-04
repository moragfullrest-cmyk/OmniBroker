namespace OmniBroker.RabbitMQ.ServiceSetup;

public class RabbitMQSettings
{
    public required string HostName { get; init; }
    public required string UserName { get; init; }
    public required string Password { get; init; }
}
