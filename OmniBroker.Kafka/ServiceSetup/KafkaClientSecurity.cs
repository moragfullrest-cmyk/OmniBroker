using Confluent.Kafka;

namespace OmniBroker.Kafka.ServiceSetup;

internal static class KafkaClientSecurity
{
    internal static void Apply(KafkaSettings settings, ClientConfig config)
    {
        if (settings.SecurityProtocol is { } protocol)
            config.SecurityProtocol = protocol;
        if (settings.SaslMechanism is { } mechanism)
            config.SaslMechanism = mechanism;
        if (settings.SaslUsername is not null)
            config.SaslUsername = settings.SaslUsername;
        if (settings.SaslPassword is not null)
            config.SaslPassword = settings.SaslPassword;
    }
}
