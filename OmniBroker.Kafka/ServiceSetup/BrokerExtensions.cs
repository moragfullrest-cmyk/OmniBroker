using OmniBroker.Kafka.Implementations;

namespace OmniBroker.Kafka.ServiceSetup;

public static class BrokerExtensions
{
    /// <summary>
    /// Configure Kafka connection
    /// </summary>
    public static BrokerOptionsBuilder UseKafka(this BrokerOptionsBuilder optionsBuilder, KafkaSettings settings)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(settings);

        optionsBuilder.Extension = new KafkaExtension(settings);
        optionsBuilder.NameResolver ??= new KafkaNameResolver();
        return optionsBuilder;
    }
}
