using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.Implementations;
using RabbitMQ.Client;

namespace OmniBroker.RabbitMQ.ServiceSetup;

internal sealed class RabbitMQExtension(RabbitMQSettings settings) : IBrokerExtension
{
    private readonly HashSet<Type> _replyTypes = [];
    public BrokerId BrokerId { get; set; }
    public string ReplyQueueName { get; set; } = null!;
    public bool SupportsRpc => true;

    public Task SetupInfrastructure(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        services.AddKeyedSingleton(BrokerId, settings);
        services.AddKeyedSingleton(BrokerId, new RabbitMqRuntime());

        services.AddKeyedSingleton(BrokerId, (s, _) =>
        {
            RabbitMqRuntime runtime = s.GetRequiredKeyedService<RabbitMqRuntime>(BrokerId);
            return runtime.RequireConnection();
        });

        services.AddKeyedSingleton(BrokerId, (s, _) =>
        {
            RabbitMqRuntime runtime = s.GetRequiredKeyedService<RabbitMqRuntime>(BrokerId);
            return runtime.RequireChannelPool();
        });

        services.AddKeyedSingleton<INameResolver>(BrokerId, builder.NameResolver!);

        return Task.CompletedTask;
    }

    public Task SetupConsumers(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        foreach (KeyValuePair<Type, List<Func<IServiceProvider, IMessage, MessageContext, Task<bool>>>> handler in builder.Consumables)
        {
            services.AddKeyedSingleton(builder.BrokerId, new HandlerWrapper(
                MessageType: handler.Key,
                Handlers: handler.Value,
                CreateMessage: HandlerWrapper.BuildCreateMessage(handler.Key)));
        }

        return Task.CompletedTask;
    }

    public Task SetupRpc(IServiceCollection services, BrokerOptionsBuilder builder)
    {
        services.AddKeyedSingleton(BrokerId, new ConcurrentDictionary<string, TaskCompletionSource<IMessage>>());

        foreach (var t in builder.RpcCallers)
        {
            services.AddTransient(typeof(IRpcCaller<,>).MakeGenericType(t.Key, t.Value), (s) =>
            {
                return Activator.CreateInstance(typeof(RabbitMQBasicRpcCaller<,>).MakeGenericType(t.Key, t.Value), s, builder.BrokerId)!;
            });
            services.AddKeyedScoped(typeof(IProducer<>).MakeGenericType(t.Key), BrokerId, (s, obj) =>
            {
                Type producerType = typeof(RabbitMQBasicProducer<>).MakeGenericType(t.Key);
                return Activator.CreateInstance(producerType,
                    s.GetRequiredService(typeof(ILogger<>).MakeGenericType(producerType)),
                    s.GetRequiredKeyedService<ConcurrentObjectPool<IChannel>>(obj),
                    s.GetRequiredKeyedService<INameResolver>(obj))!;
            });

            services.AddKeyedSingleton(builder.BrokerId, new HandlerWrapper
            (
                MessageType: t.Value,
                Handlers:
                [
                    OmniBroker.BrokerExtensions.WrapActionDelegate<bool>((Delegate)typeof(RabbitMQExtension).GetMethod(nameof(CreateCorrelationDelegate)
                    , System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!.MakeGenericMethod(t.Value)
                    .Invoke(null, null)!)
                ],
                CreateMessage: HandlerWrapper.BuildCreateMessage(t.Value)
            ));
            _replyTypes.Add(t.Value);
        }

        foreach (var t in builder.RpcReceivers)
        {
            var returnType = t.Value.Method.ReturnType.GenericTypeArguments[0];
            services.AddKeyedSingleton(builder.BrokerId, new HandlerWrapper
            (
                MessageType: t.Key,
                Handlers:
                [
                    OmniBroker.BrokerExtensions.WrapActionDelegate<bool>((Delegate)typeof(RabbitMQExtension).GetMethod(nameof(CreateReplyDelegate)
                    , System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!.MakeGenericMethod(returnType)
                    .Invoke(null, [t.Value])!)
                ],
                CreateMessage: HandlerWrapper.BuildCreateMessage(t.Key)
            ));
            services.AddKeyedScoped(typeof(IProducer<>).MakeGenericType(returnType), BrokerId, (s, obj) =>
            {
                Type producerType = typeof(RabbitMQBasicProducer<>).MakeGenericType(returnType);
                return Activator.CreateInstance(producerType,
                    s.GetRequiredService(typeof(ILogger<>).MakeGenericType(producerType)),
                    s.GetRequiredKeyedService<ConcurrentObjectPool<IChannel>>(obj),
                    s.GetRequiredKeyedService<INameResolver>(obj))!;
            });
        }

        return Task.CompletedTask;
    }

    private static Func<ConcurrentDictionary<string, TaskCompletionSource<IMessage>>, TOutput, Task<bool>> CreateCorrelationDelegate<TOutput>()
        where TOutput : IMessage
    {
        return (operations, message) =>
        {
            if (string.IsNullOrEmpty(message.CorrelationId))
                return Task.FromResult(false);

            if (operations.TryRemove(message.CorrelationId, out TaskCompletionSource<IMessage>? tcs))
            {
                tcs.TrySetResult(message);
            }
            return Task.FromResult(true);
        };
    }

    private static Func<IServiceProvider, IMessage, MessageContext, Task<bool>> CreateReplyDelegate<TOutput>(Delegate action)
        where TOutput : IMessage
    {
        var handler = OmniBroker.BrokerExtensions.WrapActionDelegate<TOutput>(action);
        return async (IServiceProvider provider, IMessage message, MessageContext context) =>
        {
            TOutput result = await handler(provider, message, context);
            if (result is null)
                throw new InvalidOperationException("RPC reply message must not be null.");

            var producer = provider.GetRequiredKeyedService<IProducer<TOutput>>(context.CurrentBrokerId);
            var builder = provider.GetRequiredKeyedService<BrokerOptionsBuilder>(context.CurrentBrokerId);
            if (context.ReplyInfo is not RabbitMQReplyInfo replyInfo
                || string.IsNullOrEmpty(replyInfo.ReplyTo))
            {
                throw new InvalidOperationException("RPC reply requires ReplyInfo.ReplyTo.");
            }
            result.Tag = replyInfo.ReplyTo;
            return await producer.Publish(result, new PublishOptions
            (
                CorrelationId: message.CorrelationId,
                Destination: builder.NameResolver!.ResolveOutboundName(typeof(TOutput)),
                ReplyTo: null
            ));
        };
    }

    public Task SetupProducers(IServiceCollection services, BrokerOptionsBuilder builder)
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
                    s.GetRequiredService(typeof(ILogger<>).MakeGenericType(producerType)),
                    s.GetRequiredKeyedService<ConcurrentObjectPool<IChannel>>(obj),
                    s.GetRequiredKeyedService<INameResolver>(obj))!;
            });
        }

        return Task.CompletedTask;
    }

    public async Task StartInfrastructure(IServiceProvider serviceProvider, BrokerOptionsBuilder builder)
    {
        RabbitMqRuntime runtime = serviceProvider.GetRequiredKeyedService<RabbitMqRuntime>(BrokerId);

        if (runtime.Connection is null)
        {
            ConnectionFactory factory = new()
            {
                UserName = settings.UserName,
                Password = settings.Password,
                HostName = settings.HostName,
                VirtualHost = settings.VirtualHost,
                Port = settings.Port
            };

            if (settings.UseTls)
            {
                factory.Ssl = new SslOption
                {
                    Enabled = true,
                    ServerName = settings.HostName
                };
            }

            IConnection connection = await factory.CreateConnectionAsync();
            runtime.Connection = connection;

            runtime.ChannelPool = new ConcurrentObjectPool<IChannel>(
                ct => connection.CreateChannelAsync(cancellationToken: ct),
                settings.MaxChannelPoolSize);
        }

        INameResolver nameResolver = serviceProvider.GetRequiredKeyedService<INameResolver>(BrokerId);
        await TopologyDeclarer.EnsureProducersDeclared(runtime.Connection, nameResolver, builder);
        await TopologyDeclarer.EnsureConsumersDeclared(runtime.Connection, nameResolver, builder);
        await TopologyDeclarer.EnsureRpcDeclared(runtime.Connection, nameResolver, builder);
    }

    internal async Task RecoverAsync(IServiceProvider serviceProvider, BrokerOptionsBuilder builder)
    {
        RabbitMqRuntime runtime = serviceProvider.GetRequiredKeyedService<RabbitMqRuntime>(BrokerId);
        await runtime.ReconnectLock.WaitAsync();
        try
        {
            if (runtime.Connection is { IsOpen: true } && runtime.ConsumerChannel is { IsOpen: true })
            {
                return;
            }

            CancelPendingRpc(serviceProvider);
            await runtime.ResetConnectionAsync();
            await StartInfrastructure(serviceProvider, builder);
            await StartConsumers(serviceProvider, builder);
        }
        finally
        {
            runtime.SuppressConsumerShutdownRecover = false;
            runtime.ReconnectLock.Release();
        }
    }

    private void CancelPendingRpc(IServiceProvider serviceProvider)
    {
        var pending = serviceProvider.GetRequiredKeyedService<ConcurrentDictionary<string, TaskCompletionSource<IMessage>>>(BrokerId);
        foreach (var pair in pending)
        {
            if (pending.TryRemove(pair.Key, out TaskCompletionSource<IMessage>? tcs))
            {
                tcs.TrySetException(new InvalidOperationException(
                    "RabbitMQ connection recovered; pending RPC cancelled."));
            }
        }
    }

    public async Task StartConsumers(IServiceProvider services, BrokerOptionsBuilder builder)
    {
        IEnumerable<HandlerWrapper> handlers = services.GetKeyedServices<HandlerWrapper>(BrokerId);
        if (handlers.Any() == false && builder.RpcCallers.Count == 0)
            return;

        RabbitMqRuntime runtime = services.GetRequiredKeyedService<RabbitMqRuntime>(BrokerId);
        IConnection connection = runtime.RequireConnection();
        IChannel channel = await connection.CreateChannelAsync();
        runtime.ConsumerChannel = channel;

        await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);
        INameResolver resolver = services.GetRequiredKeyedService<INameResolver>(BrokerId);
        var consumer = new RabbitMQBasicConsumer(
            services,
            channel,
            builder.BrokerId,
            services.GetRequiredService<ILogger<RabbitMQBasicConsumer>>());

        bool replyQueueConsumed = false;
        foreach (HandlerWrapper handler in handlers)
        {
            if (_replyTypes.Contains(handler.MessageType))
            {
                if (!replyQueueConsumed)
                {
                    await channel.BasicConsumeAsync(
                                queue: ReplyQueueName,
                                autoAck: false,
                                consumer: consumer,
                                CancellationToken.None);
                    replyQueueConsumed = true;
                }
            }
            else
            {
                await channel.BasicConsumeAsync(
                            queue: resolver.ResolveInboundName(handler.MessageType),
                            autoAck: false,
                            consumer: consumer,
                            CancellationToken.None);
            }
        }

        if (builder.RpcCallers.Count > 0 && !replyQueueConsumed)
        {
            await channel.BasicConsumeAsync(
                queue: ReplyQueueName,
                autoAck: false,
                consumer: consumer,
                CancellationToken.None);
        }
    }
}
