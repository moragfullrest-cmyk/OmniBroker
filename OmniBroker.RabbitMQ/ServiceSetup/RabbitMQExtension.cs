using System.Collections.Concurrent;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.Implementations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace OmniBroker.RabbitMQ.ServiceSetup;

internal class RabbitMQExtension(RabbitMQSettings settings) : IBrokerExtension
{
    private readonly HashSet<Type> _replyTypes = [];
    public BrokerId BrokerId { get; set; }
    public string ReplyQueueName;

    public async Task SetupInfrastructure(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        ConnectionFactory factory = new()
        {
            UserName = settings.UserName,
            Password = settings.Password,
            HostName = settings.HostName
        };

        services.AddKeyedSingleton(BrokerId, (s, _) =>
        {
            IConnection connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
            return connection;
        });

        services.AddKeyedSingleton(BrokerId, (s, _) =>
        {
            return new ConcurrentObjectPool<IChannel>(() =>
            {
                IConnection connection = s.GetRequiredKeyedService<IConnection>(BrokerId);
                IChannel channel = connection.CreateChannelAsync().GetAwaiter().GetResult();
                channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false).GetAwaiter().GetResult();
                return channel;
            });
        });

        services.AddKeyedSingleton<INameResolver>(BrokerId, builder.NameResolver);
    }

    public async Task SetupConsumers(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        foreach (KeyValuePair<Type, List<Func<IServiceProvider, IMessage, MessageContext, Task<bool>>>> handler in builder.Consumables)
        {
            services.AddKeyedSingleton(builder.BrokerId, new HandlerWrapper { MessageType = handler.Key, Handlers = handler.Value });
        }
    }

    public async Task SetupRpc(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        services.AddKeyedSingleton(BrokerId, new ConcurrentDictionary<string, PendingOperation>());

        foreach (var t in builder.RpcCallers)
        {
            services.AddTransient(typeof(IRpcCaller<,>).MakeGenericType(t.Key, t.Value), (s) =>
            {
                return Activator.CreateInstance(typeof(RabbitMQBasicRpcCaller<,>).MakeGenericType(t.Key, t.Value), s, builder.BrokerId);
            });
            services.AddKeyedScoped(typeof(IProducer<>).MakeGenericType(t.Key), BrokerId, (s, obj) =>
            {
                Type producerType = typeof(RabbitMQBasicProducer<>).MakeGenericType(t.Key);
                return Activator.CreateInstance(producerType,
                    s.GetService(typeof(ILogger<>).MakeGenericType(producerType)),
                    s.GetRequiredKeyedService<ConcurrentObjectPool<IChannel>>(obj),
                    s.GetRequiredKeyedService<INameResolver>(obj));
            });

            services.AddKeyedSingleton(builder.BrokerId, new HandlerWrapper
            {
                MessageType = t.Value,
                Handlers =
                [
                    Broker.BrokerExtensions.WrapActionDelegate<bool>((Delegate )typeof(RabbitMQExtension).GetMethod(nameof(CreateCorrelationDelegate)
                    , System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic).MakeGenericMethod(t.Value)
                    .Invoke(null, null))
                ]
            });
            _replyTypes.Add(t.Value);
        }

        foreach (var t in builder.RpcReceivers)
        {
            var returnType = t.Value.Method.ReturnType.GenericTypeArguments[0];
            services.AddKeyedSingleton(builder.BrokerId, new HandlerWrapper
            {
                MessageType = t.Key,
                Handlers =
                [
                    Broker.BrokerExtensions.WrapActionDelegate<bool>((Delegate )typeof(RabbitMQExtension).GetMethod(nameof(CreateReplyDelegate)
                    , System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic).MakeGenericMethod(returnType)
                    .Invoke(null, new object[]{t.Value }))
                ]
            });
            services.AddKeyedScoped(typeof(IProducer<>).MakeGenericType(returnType), BrokerId, (s, obj) =>
            {
                Type producerType = typeof(RabbitMQBasicProducer<>).MakeGenericType(returnType);
                return Activator.CreateInstance(producerType,
                    s.GetService(typeof(ILogger<>).MakeGenericType(producerType)),
                    s.GetRequiredKeyedService<ConcurrentObjectPool<IChannel>>(obj),
                    s.GetRequiredKeyedService<INameResolver>(obj));
            });
        }
    }

    private static Func<ConcurrentDictionary<string, PendingOperation>, TOutput, Task<bool>> CreateCorrelationDelegate<TOutput>()
        where TOutput : IMessage
    {
        return (ConcurrentDictionary<string, PendingOperation> operations, TOutput message) =>
        {
            if (operations.ContainsKey(message.CorrelationId))
            {
                PendingOperation op = operations[message.CorrelationId];
                if (op.Timedout)
                {
                    operations.TryRemove(message.CorrelationId, out _);
                }
                else
                {
                    operations.TryUpdate(message.CorrelationId, op with { Result = message }, op);
                }
            }
            return Task.FromResult(true);
        };
    }

    private static Func<IServiceProvider, IMessage, MessageContext, Task<bool>> CreateReplyDelegate<TOutput>(Delegate action)
        where TOutput : IMessage
    {
        return async (IServiceProvider provider, IMessage message, MessageContext context) =>
        {
            TOutput result = await Broker.BrokerExtensions.WrapActionDelegate<TOutput>(action)(provider, message, context);
            var producer = (RabbitMQBasicProducer<TOutput>)provider.GetRequiredKeyedService<IProducer<TOutput>>(context.CurrentBrokerId);
            producer.CorrelationId = message.CorrelationId;
            var builder = provider.GetRequiredKeyedService<BrokerOptionsBuilder>(context.CurrentBrokerId);
            producer.ExchangeName = builder.NameResolver.ResolveOutboundName(typeof(TOutput));
            result.Tag = ((RabbitMQReplyInfo)context.ReplyInfo).ReplyTo;
            return await producer.Publish(result);
        };
    }

    public async Task SetupProducers(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        foreach (Type type in builder.Producables)
        {
            if (services.Any(_ => _.ServiceType == typeof(MultiBrokerProducer<>).MakeGenericType(type)) == false)
            {
                services.AddScoped(typeof(IProducer<>).MakeGenericType(type), typeof(MultiBrokerProducer<>).MakeGenericType(type));
            }
            services.AddKeyedScoped(typeof(IProducer<>).MakeGenericType(type), BrokerId, (s, obj) =>
            {
                Type producerType = typeof(RabbitMQBasicProducer<>).MakeGenericType(type);
                return Activator.CreateInstance(producerType,
                    s.GetService(typeof(ILogger<>).MakeGenericType(producerType)),
                    s.GetRequiredKeyedService<ConcurrentObjectPool<IChannel>>(obj),
                    s.GetRequiredKeyedService<INameResolver>(obj));
            });
        }
    }

    public async Task StartInfrastructure(IServiceProvider serviceProvider, BrokerOptionsBuilder builder)
    {
        IConnection connection = serviceProvider.GetRequiredKeyedService<IConnection>(BrokerId);
        INameResolver nameResolver = serviceProvider.GetRequiredKeyedService<INameResolver>(BrokerId);
        await TopologyDeclarer.EnsureProducersDeclared(connection, nameResolver, builder);
        await TopologyDeclarer.EnsureConsumersDeclared(connection, nameResolver, builder);
        await TopologyDeclarer.EnsureRpcDeclared(connection, nameResolver, builder);
    }

    public async Task StartConsumers(IServiceProvider services, BrokerOptionsBuilder builder)
    {
        INameResolver resolver = services.GetRequiredKeyedService<INameResolver>(BrokerId);
        IChannel channel = services.GetRequiredKeyedService<ConcurrentObjectPool<IChannel>>(BrokerId).Get();
        IEnumerable<HandlerWrapper> handlers = services.GetKeyedServices<HandlerWrapper>(BrokerId);
        if (handlers.Any() == false)
            return;
        var consumer = new RabbitMQBasicConsumer(services, channel, builder.BrokerId);
        BrokerOptionsBuilder optionsBuilder = services.GetServices<BrokerOptionsBuilder>().First(_ => _.BrokerId == BrokerId);

        foreach (HandlerWrapper handler in handlers)
        {
            if (_replyTypes.Contains(handler.MessageType))
            {
                await channel.BasicConsumeAsync(
                            queue: ReplyQueueName,
                            autoAck: true,
                            consumer: consumer,
                            CancellationToken.None);
            }
            else
            {
                await channel.BasicConsumeAsync(
                            queue: resolver.ResolveInboundName(handler.MessageType),
                            autoAck: true,
                            consumer: consumer,
                            CancellationToken.None);
            }
        }

        if (builder.RpcCallers.Count > 0)
        {
            await channel.BasicConsumeAsync(
                queue: ((RabbitMQExtension)optionsBuilder.Extension).ReplyQueueName,
                autoAck: true,
                consumer: consumer,
                CancellationToken.None);
        }

    }
}
