using OmniBroker.RabbitMQ.Implementations;
using OmniBroker.RabbitMQ.ServiceSetup;

namespace OmniBroker.RabbitMQ;

public static class BrokerExtensions
{
    /// <summary>
    /// Метод настройки подключения к RabbitMQ
    /// </summary>
    public static BrokerOptionsBuilder UseRabbitMq(this BrokerOptionsBuilder optionsBuilder, RabbitMQSettings settings)
    {
        optionsBuilder.Extension = new RabbitMQExtension(settings);
        optionsBuilder.NameResolver ??= new RabbitMQNameResolver(optionsBuilder.SetupName);
        return optionsBuilder;
    }
}
