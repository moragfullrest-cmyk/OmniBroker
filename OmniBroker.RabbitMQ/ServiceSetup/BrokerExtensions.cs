using OmniBroker.RabbitMQ.Implementations;
using OmniBroker.RabbitMQ.ServiceSetup;

namespace OmniBroker.RabbitMQ;

public static class BrokerExtensions
{
    /// <summary>
    /// Configure RabbitMQ connection
    /// </summary>
    public static BrokerOptionsBuilder UseRabbitMq(this BrokerOptionsBuilder optionsBuilder, RabbitMQSettings settings)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(settings);

        optionsBuilder.Extension = new RabbitMQExtension(settings);
        optionsBuilder.NameResolver ??= new RabbitMQNameResolver(optionsBuilder.SetupName);
        return optionsBuilder;
    }
}
