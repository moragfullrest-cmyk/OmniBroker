using System.Linq.Expressions;
using System.Reflection;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.ServiceSetup;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OmniBroker.RabbitMQ.Implementations;

internal class RabbitMQBasicConsumer : AsyncDefaultBasicConsumer
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IChannel _channel;
    private readonly Dictionary<string, HandlerWrapper> _handlers;
    private readonly Dictionary<string, Func<IMessage>> _messageActivators = new Dictionary<string, Func<IMessage>>();

    private readonly BrokerId _brokerId;

    public RabbitMQBasicConsumer(IServiceProvider serviceProvider, IChannel channel, BrokerId id) : base(channel)
    {
        _serviceProvider = serviceProvider;
        INameResolver nameResolver = _serviceProvider.GetRequiredKeyedService<INameResolver>(id);
        _channel = channel;
        _handlers = _serviceProvider.GetKeyedServices<HandlerWrapper>(id).ToDictionary(_ => nameResolver.ResolveOutboundName(_.MessageType));
        _brokerId = id;

        foreach (KeyValuePair<string, HandlerWrapper> handler in _handlers)
        {
            ConstructorInfo? ctor = handler.Value.MessageType.GetConstructor([]);
            if (ctor == null)
            {
                throw new MissingMethodException(handler.Value.MessageType.FullName, "Сообщение должно иметь конструктор без параметров");
            }
            NewExpression newExp = Expression.New(ctor);

            var expr = Expression.Lambda<Func<IMessage>>(newExp);

            _messageActivators.Add(handler.Key, expr.Compile());
        }
    }
    public override async Task HandleBasicDeliverAsync(
        string consumerTag,
        ulong deliveryTag,
        bool redelivered,
        string exchange,
        string routingKey,
        IReadOnlyBasicProperties properties,
        ReadOnlyMemory<byte> body,
        CancellationToken cancellationToken = default)
    {

        if (_handlers.TryGetValue(exchange, out HandlerWrapper? handler))
        {
            var scope = _serviceProvider.CreateScope();
            IMessage message = _messageActivators[exchange]();
            message.Body = body.ToArray();
            message.CorrelationId = properties.CorrelationId;
            bool result = true;
            foreach (Func<IServiceProvider, IMessage, MessageContext, Task<bool>> _delegate in handler.Handlers)
            {
                result &= await _delegate(scope.ServiceProvider, message, new MessageContext
                {
                    CurrentBrokerId = _brokerId,
                    ReplyInfo = new RabbitMQReplyInfo { ReplyTo = properties.ReplyTo }
                });
            }
        }
    }

    public override Task HandleChannelShutdownAsync(object channel, ShutdownEventArgs reason)
    {
        BrokerOptionsBuilder builder = _serviceProvider.GetServices<BrokerOptionsBuilder>().First(_ => _.BrokerId == _brokerId);
        builder.Extension.StartInfrastructure(_serviceProvider, builder);
        builder.Extension.StartConsumers(_serviceProvider, builder);
        return base.HandleChannelShutdownAsync(channel, reason);
    }
}
