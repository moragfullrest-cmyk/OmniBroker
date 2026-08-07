using Confluent.Kafka;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.Implementations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OmniBroker.Kafka.ServiceSetup;

internal class KafkaExtension(KafkaSettings settings) : IBrokerExtension
{
    public BrokerId BrokerId { get; set; }

    public async Task SetupConsumers(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        if (builder.Consumables.Count == 0)
            return;

        services.AddKeyedSingleton(serviceKey: BrokerId, implementationFactory: (sp, o) =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = settings.Hosts,
                RetryBackoffMs = 100,
                GroupId = settings.GroupId ?? builder.SetupName,
                EnableAutoCommit = false,
                AutoOffsetReset = settings.AutoOffsetReset,
            };
            if (settings.ClientId is not null)
                config.ClientId = settings.ClientId;
            ApplySecurity(config);
            return new ConsumerBuilder<string, byte[]>(config).Build();
        });

        foreach (KeyValuePair<Type, List<Func<IServiceProvider, IMessage, MessageContext, Task<bool>>>> handler in builder.Consumables)
        {
            services.AddKeyedSingleton(builder.BrokerId, new HandlerWrapper { MessageType = handler.Key, Handlers = handler.Value });
        }

        services.AddSingleton<IHostedService, KafkaConsumer>((s) => new KafkaConsumer(s, BrokerId, s.GetRequiredService<ILogger<KafkaConsumer>>(), s.GetRequiredKeyedService<INameResolver>(BrokerId)));
    }
    public async Task SetupInfrastructure(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder.NameResolver);
        services.AddKeyedSingleton<INameResolver>(builder.BrokerId, builder.NameResolver);
    }
    public async Task SetupProducers(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        if (builder.Producables.Count == 0)
            return;

        services.AddKeyedSingleton<IProducer<string, byte[]>>(serviceKey: BrokerId, implementationFactory: (sp, o) =>
        {
            var config = new ProducerConfig
            {
                BootstrapServers = settings.Hosts,
                Acks = Acks.All,
                EnableIdempotence = true,
                MessageSendMaxRetries = 3,
                RetryBackoffMs = 100
            };
            if (settings.ClientId is not null)
                config.ClientId = settings.ClientId;
            ApplySecurity(config);
            return new ProducerBuilder<string, byte[]>(config).Build();
        });

        foreach (Type type in builder.Producables)
        {
            if (services.Any(_ => _.ServiceType == typeof(MultiBrokerProducer<>).MakeGenericType(type)) == false)
            {
                services.AddScoped(typeof(IProducer<>).MakeGenericType(type), typeof(MultiBrokerProducer<>).MakeGenericType(type));
            }
            services.AddKeyedScoped(typeof(IProducer<>).MakeGenericType(type), BrokerId, (s, obj) =>
            {
                Type producerType = typeof(KafkaProducer<>).MakeGenericType(type);
                return Activator.CreateInstance(producerType,
                    s.GetRequiredService(typeof(ILogger<>).MakeGenericType(producerType)),
                    s.GetRequiredKeyedService(typeof(IProducer<string, byte[]>), obj),
                    s.GetRequiredKeyedService<INameResolver>(obj)
                    );
            });
        }

    }

    public async Task SetupRpc(IServiceCollection services, BrokerOptionsBuilder builder)
    { }

    public async Task StartConsumers(IServiceProvider services, BrokerOptionsBuilder builder)
    { }
    public async Task StartInfrastructure(IServiceProvider serviceProvider, BrokerOptionsBuilder builder)
    { }

    private void ApplySecurity(ClientConfig config)
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
