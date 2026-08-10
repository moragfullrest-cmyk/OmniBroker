using OmniBroker.Kafka.Implementations;

namespace OmniBroker.Kafka.ServiceSetup;

public static class BrokerExtensions
{
    /// <summary>
    /// Метод настройки подключения к Kafka
    /// </summary>
    public static BrokerOptionsBuilder UseKafka(this BrokerOptionsBuilder optionsBuilder, KafkaSettings settings)
    {
        optionsBuilder.Extension = new KafkaExtension(settings);
        optionsBuilder.NameResolver ??= new KafkaNameResolver();
        return optionsBuilder;
    }
}
