using System.Collections.Concurrent;
using System.Reflection;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OmniBroker;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.Implementations;

namespace OmniBroker.Kafka.ServiceSetup;

internal sealed class KafkaExtension(KafkaSettings settings, string brokerName) : IBrokerExtension
{
    private bool _consumerRegistered;

    public string BrokerName { get; } = brokerName;
    public bool SupportsRpc => true;

    public Task SetupConsumers(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        if (builder.Consumables.Count == 0)
            return Task.CompletedTask;

        EnsureKafkaConsumer(services, builder);

        foreach (KeyValuePair<Type, List<Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>>>> handler in builder.Consumables)
        {
            services.AddKeyedSingleton(builder.BrokerName, new HandlerWrapper(
                MessageType: handler.Key,
                Handlers: handler.Value,
                CreateMessage: HandlerWrapper.BuildCreateMessage(handler.Key)));
        }

        return Task.CompletedTask;
    }
    public Task SetupInfrastructure(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder.NameResolver);
        BrokerWireNames.EnsureDistinct(builder, ReservedTopics());
        services.AddKeyedSingleton<INameResolver>(builder.BrokerName, builder.NameResolver);
        services.AddSingleton<IHostedService>(s => new KafkaBrokerHostedService(s, BrokerName));
        if (builder.RpcCallers.Count > 0)
        {
            services.AddKeyedSingleton(BrokerName, new KafkaRpcReplyLease());
            services.AddSingleton<IHostedService>(s => new KafkaRpcReplyLeaseHostedService(s, BrokerName, settings));
        }

        return Task.CompletedTask;
    }
    public Task SetupProducers(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        if (builder.Producables.Count > 0 || !string.IsNullOrWhiteSpace(settings.DeadLetterTopic))
            EnsureRawProducer(services);

        foreach (Type type in builder.Producables)
            RegisterTypedProducer(services, type);

        return Task.CompletedTask;
    }

    public Task SetupRpc(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        if (builder.RpcCallers.Count > 0)
        {
            services.AddKeyedSingleton(BrokerName, settings);
            services.AddKeyedSingleton(BrokerName, new ConcurrentDictionary<string, KafkaRpcWait>());
            EnsureRawProducer(services);

            foreach (KeyValuePair<Type, Type> caller in builder.RpcCallers)
            {
                Type rpcCallerServiceType = typeof(IRpcCaller<,>).MakeGenericType(caller.Key, caller.Value);
                object CreateRpcCaller(IServiceProvider provider) =>
                    Activator.CreateInstance(
                        typeof(KafkaBasicRpcCaller<,>).MakeGenericType(caller.Key, caller.Value),
                        provider,
                        builder.BrokerName)!;

                services.AddTransient(rpcCallerServiceType, CreateRpcCaller);
                services.AddKeyedTransient(rpcCallerServiceType, BrokerName, (provider, _) => CreateRpcCaller(provider));
                RegisterTypedProducer(services, caller.Key);
            }
        }

        if (builder.RpcReceivers.Count == 0)
            return Task.CompletedTask;

        EnsureRawProducer(services);
        if (builder.Consumables.Count == 0)
            EnsureKafkaConsumer(services, builder);

        foreach (KeyValuePair<Type, Delegate> receiver in builder.RpcReceivers)
        {
            Type returnType = receiver.Value.Method.ReturnType.GenericTypeArguments[0];
            Delegate reply = (Delegate)typeof(KafkaExtension)
                .GetMethod(nameof(CreateReplyDelegate), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(returnType)
                .Invoke(null, [receiver.Value])!;
            services.AddKeyedSingleton(builder.BrokerName, new HandlerWrapper(
                MessageType: receiver.Key,
                Handlers: [OmniBroker.BrokerExtensions.WrapActionDelegate<HandleResult>(reply)],
                CreateMessage: HandlerWrapper.BuildCreateMessage(receiver.Key)));
            RegisterTypedProducer(services, returnType);
        }

        return Task.CompletedTask;
    }

    private static Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> CreateReplyDelegate<TOutput>(Delegate action)
        where TOutput : IMessage
    {
        Func<IServiceProvider, IMessage, MessageContext, Task<TOutput>> handler =
            OmniBroker.BrokerExtensions.WrapActionDelegate<TOutput>(action);
        return async (IServiceProvider provider, IMessage message, MessageContext context) =>
        {
            TOutput result = await handler(provider, message, context);
            if (result is null)
                throw new InvalidOperationException("RPC reply message must not be null.");

            if (context.ReplyInfo is not KafkaReplyInfo replyInfo
                || string.IsNullOrWhiteSpace(replyInfo.ReplyTopic)
                || replyInfo.ReplyPartition is not int partition)
            {
                throw new InvalidOperationException("RPC reply requires reply topic and partition.");
            }

            OmniBroker.IProducer<TOutput> producer = provider.GetRequiredKeyedService<OmniBroker.IProducer<TOutput>>(context.CurrentBrokerName);
            bool published = await producer.Publish(result, new KafkaPublishOptions(
                CorrelationId: message.CorrelationId,
                ReplyTo: null,
                Destination: replyInfo.ReplyTopic,
                ReplyPartition: partition), context.CancellationToken);
            return published ? HandleResult.Ack : HandleResult.Retry;
        };
    }

    private void EnsureKafkaConsumer(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        if (_consumerRegistered)
            return;

        _consumerRegistered = true;
        services.AddKeyedSingleton(serviceKey: BrokerName, implementationFactory: (IServiceProvider _, object _) =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = settings.Hosts,
                RetryBackoffMs = 100,
                GroupId = settings.GroupId ?? builder.BrokerName,
                EnableAutoCommit = false,
                AutoOffsetReset = settings.AutoOffsetReset,
            };
            if (settings.ClientId is not null)
                config.ClientId = settings.ClientId;
            KafkaClientSecurity.Apply(settings, config);
            return new ConsumerBuilder<string, byte[]>(config).Build();
        });

        services.Add(HostedService<KafkaConsumer>(s => new KafkaConsumer(
            s,
            BrokerName,
            s.GetRequiredService<ILogger<KafkaConsumer>>(),
            s.GetRequiredKeyedService<INameResolver>(BrokerName),
            settings)));
    }

    private static ServiceDescriptor HostedService<TService>(Func<IServiceProvider, TService> factory)
        where TService : class, IHostedService
    {
        // AddSingleton<IHostedService, T>(factory) stores only the delegate, so ImplementationType stays null.
        ServiceDescriptor descriptor = ServiceDescriptor.Singleton<IHostedService>(factory);
        typeof(ServiceDescriptor)
            .GetField("_implementationType", BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(descriptor, typeof(TService));
        return descriptor;
    }

    private void EnsureRawProducer(IServiceCollection services)
    {
        if (services.Any(descriptor => descriptor.IsKeyedService
            && Equals(descriptor.ServiceKey, BrokerName)
            && descriptor.ServiceType == typeof(IProducer<string, byte[]>)))
        {
            return;
        }

        services.AddKeyedSingleton<IProducer<string, byte[]>>(serviceKey: BrokerName, implementationFactory: (IServiceProvider _, object _) =>
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
            KafkaClientSecurity.Apply(settings, config);
            return new ProducerBuilder<string, byte[]>(config).Build();
        });
    }

    private void RegisterTypedProducer(IServiceCollection services, Type messageType)
    {
        services.TryAddMultiBrokerProducer(messageType);
        services.AddKeyedMessageProducer(messageType, BrokerName, (provider, key) =>
        {
            Type producerType = typeof(KafkaProducer<>).MakeGenericType(messageType);
            return Activator.CreateInstance(producerType,
                provider.GetRequiredService(typeof(ILogger<>).MakeGenericType(producerType)),
                provider.GetRequiredKeyedService(typeof(IProducer<string, byte[]>), key),
                provider.GetRequiredKeyedService<INameResolver>(key))!;
        });
    }

    private IEnumerable<BrokerWireNames.ReservedName> ReservedTopics()
    {
        if (!string.IsNullOrWhiteSpace(settings.DeadLetterTopic))
            yield return new BrokerWireNames.ReservedName("DeadLetterTopic", settings.DeadLetterTopic);
        if (!string.IsNullOrWhiteSpace(settings.RpcReplyTopic))
            yield return new BrokerWireNames.ReservedName("RpcReplyTopic", settings.RpcReplyTopic);
    }

    public async Task ValidateRemoteBrokerState(
        IServiceProvider serviceProvider,
        BrokerOptionsBuilder builder,
        CancellationToken cancellationToken = default)
    {
        using var metadata = new KafkaAdminMetadataClient(settings);
        await ValidateRemoteBrokerState(serviceProvider, builder, metadata, cancellationToken);
    }

    internal async Task ValidateRemoteBrokerState(
        IServiceProvider serviceProvider,
        BrokerOptionsBuilder builder,
        IKafkaMetadataClient metadata,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(metadata);
        if (builder.RpcCallers.Count > 0 && string.IsNullOrWhiteSpace(settings.RpcReplyTopic))
        {
            throw new BrokerStateException(
                BrokerName,
                KafkaRpcReplyStrings.MissingTopicMessage());
        }

        try
        {
            await metadata.ProbeConnectionAsync(KafkaAdminMetadataClient.Timeout, cancellationToken);
        }
        catch (BrokerStateException)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new BrokerStateException(
                BrokerName,
                $"Kafka cluster '{settings.Hosts}' is unreachable.",
                ex);
        }

        INameResolver nameResolver = serviceProvider.GetRequiredKeyedService<INameResolver>(BrokerName);
        Dictionary<string, KafkaTopicSnapshot> snapshots = await ReadTopicsAsync(nameResolver, builder, metadata, cancellationToken);
        await EnsureReplyPartitionAsync(builder, metadata, snapshots, cancellationToken);
    }

    public Task StartConsumers(IServiceProvider services, BrokerOptionsBuilder builder, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public Task StartInfrastructure(IServiceProvider serviceProvider, BrokerOptionsBuilder builder, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    private async Task<Dictionary<string, KafkaTopicSnapshot>> ReadTopicsAsync(
        INameResolver nameResolver,
        BrokerOptionsBuilder builder,
        IKafkaMetadataClient metadata,
        CancellationToken cancellationToken)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (Type type in builder.Producables)
            names.Add(nameResolver.ResolveOutboundName(type));
        foreach (Type type in builder.Consumables.Keys)
            names.Add(nameResolver.ResolveInboundName(type));
        if (!string.IsNullOrWhiteSpace(settings.DeadLetterTopic))
            names.Add(settings.DeadLetterTopic);
        if (builder.RpcCallers.Count > 0 && !string.IsNullOrWhiteSpace(settings.RpcReplyTopic))
            names.Add(settings.RpcReplyTopic);

        var snapshots = new Dictionary<string, KafkaTopicSnapshot>(StringComparer.Ordinal);
        var missing = new List<string>();
        var unauthorized = new List<string>();
        var other = new List<string>();

        foreach (string name in names.OrderBy(name => name, StringComparer.Ordinal))
        {
            KafkaTopicSnapshot snapshot = await metadata.ReadTopicAsync(name, KafkaAdminMetadataClient.Timeout, cancellationToken);
            snapshots[name] = snapshot;
            if (snapshot.Unknown)
                missing.Add(name);
            else if (snapshot.AuthorizationFailed)
                unauthorized.Add(name);
            else if (snapshot.OtherError is not null)
                other.Add($"{name} ({snapshot.OtherError})");
        }

        if (missing.Count > 0 || unauthorized.Count > 0 || other.Count > 0)
            throw new BrokerStateException(BrokerName, FormatTopicProblems(missing, unauthorized, other));

        return snapshots;
    }

    private async Task EnsureReplyPartitionAsync(
        BrokerOptionsBuilder builder,
        IKafkaMetadataClient metadata,
        Dictionary<string, KafkaTopicSnapshot> snapshots,
        CancellationToken cancellationToken)
    {
        if (builder.RpcCallers.Count == 0)
            return;

        string topic = settings.RpcReplyTopic!;
        KafkaTopicSnapshot reply = snapshots[topic];
        bool anyFreeWithLeader = false;
        bool anyUnoccupied = false;
        foreach (KafkaPartitionSnapshot partition in reply.Partitions.OrderBy(item => item.PartitionId))
        {
            string groupId = KafkaRpcReplyStrings.ForPartition(settings, builder.BrokerName, partition.PartitionId);
            KafkaGroupSnapshot group = await metadata.ReadGroupAsync(
                groupId,
                KafkaAdminMetadataClient.Timeout,
                cancellationToken);
            if (group.Failure is not null)
            {
                throw new BrokerStateException(
                    BrokerName,
                    KafkaRpcReplyStrings.GroupFailureMessage(groupId, topic, group.Failure));
            }

            if (!KafkaRpcReplyStrings.IsFree(group))
                continue;

            anyUnoccupied = true;
            if (!partition.HasLeader)
                continue;

            anyFreeWithLeader = true;
            break;
        }

        if (anyFreeWithLeader)
            return;

        if (!anyUnoccupied)
        {
            throw new BrokerStateException(
                BrokerName,
                KafkaRpcReplyStrings.MaximumExceededMessage(topic, KafkaRpcReplyStrings.Prefix(settings, builder.BrokerName), reply.Partitions.Count));
        }

        throw new BrokerStateException(
            BrokerName,
            KafkaRpcReplyStrings.NoLeaderMessage(topic));
    }

    private string FormatTopicProblems(
        IReadOnlyList<string> missing,
        IReadOnlyList<string> unauthorized,
        IReadOnlyList<string> other)
    {
        var parts = new List<string>();
        if (missing.Count > 0)
            parts.Add($"missing topic(s): {string.Join(", ", missing)}");
        if (unauthorized.Count > 0)
            parts.Add($"authorization failed for topic(s): {string.Join(", ", unauthorized)}");
        if (other.Count > 0)
            parts.Add($"unusable topic(s): {string.Join(", ", other)}");

        return $"Kafka broker '{BrokerName}' cannot use the configured topics. {string.Join("; ", parts)}.";
    }
}
