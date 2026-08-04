using OmniBroker.Kafka.Implementations;

namespace OmniBroker.Kafka.ServiceSetup;

public static class BrokerExtensions
{
    public static BrokerOptionsBuilder UseKafka(this BrokerOptionsBuilder optionsBuilder, KafkaSettings settings)
    {
        optionsBuilder.Extension = new KafkaExtension(settings);
        optionsBuilder.NameResolver ??= new KafkaNameResolver();
        return optionsBuilder;
    }
}
