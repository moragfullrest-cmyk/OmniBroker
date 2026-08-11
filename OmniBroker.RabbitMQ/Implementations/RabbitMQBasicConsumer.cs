using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.ServiceSetup;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace OmniBroker.RabbitMQ.Implementations;

internal sealed class RabbitMQBasicConsumer : AsyncDefaultBasicConsumer
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IChannel _channel;
    private readonly Dictionary<string, HandlerWrapper> _handlers;
    private readonly ILogger<RabbitMQBasicConsumer> _logger;
    private readonly BrokerId _brokerId;

    public RabbitMQBasicConsumer(
        IServiceProvider serviceProvider,
        IChannel channel,
        BrokerId id,
        ILogger<RabbitMQBasicConsumer> logger) : base(channel)
    {
        _serviceProvider = serviceProvider;
        INameResolver nameResolver = _serviceProvider.GetRequiredKeyedService<INameResolver>(id);
        _channel = channel;
        _handlers = _serviceProvider.GetKeyedServices<HandlerWrapper>(id).ToDictionary(_ => nameResolver.ResolveOutboundName(_.MessageType));
        _brokerId = id;
        _logger = logger;
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
        if (!_handlers.TryGetValue(exchange, out HandlerWrapper? handler))
        {
            _logger.LogWarning(
                "No handler for exchange {Exchange}; nacking delivery {DeliveryTag} (routingKey={RoutingKey})",
                exchange,
                deliveryTag,
                routingKey);
            await _channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
            return;
        }

        using var scope = _serviceProvider.CreateScope();
        try
        {
            IMessage message = handler.CreateMessage();
            message.Body = body.ToArray();
            message.CorrelationId = properties.CorrelationId;
            bool result = true;
            foreach (Func<IServiceProvider, IMessage, MessageContext, Task<bool>> _delegate in handler.Handlers)
            {
                result &= await _delegate(scope.ServiceProvider, message, new MessageContext
                (
                    CurrentBrokerId: _brokerId,
                    ReplyInfo: new RabbitMQReplyInfo { ReplyTo = properties.ReplyTo }
                ));
            }

            if (result)
            {
                await _channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);
            }
            else
            {
                _logger.LogWarning(
                    "Handler returned false; nacking delivery {DeliveryTag} (exchange={Exchange}, routingKey={RoutingKey})",
                    deliveryTag,
                    exchange,
                    routingKey);
                await _channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error processing RabbitMQ message; nacking delivery {DeliveryTag} (exchange={Exchange}, routingKey={RoutingKey})",
                deliveryTag,
                exchange,
                routingKey);
            await _channel.BasicNackAsync(deliveryTag, multiple: false, requeue: false, cancellationToken);
        }
    }

    public override async Task HandleChannelShutdownAsync(object channel, ShutdownEventArgs reason)
    {
        _logger.LogWarning(
            "RabbitMQ consumer channel shutdown: ReplyCode={ReplyCode}, ReplyText={ReplyText}, Initiator={Initiator}",
            reason.ReplyCode,
            reason.ReplyText,
            reason.Initiator);

        try
        {
            RabbitMqRuntime runtime = _serviceProvider.GetRequiredKeyedService<RabbitMqRuntime>(_brokerId);

            if (runtime.SuppressConsumerShutdownRecover)
            {
                _logger.LogDebug("Skipping recover: consumer shutdown suppressed during connection reset.");
                return;
            }

            if (!ReferenceEquals(runtime.ConsumerChannel, _channel))
            {
                _logger.LogDebug("Skipping recover: shutdown is for a stale consumer channel.");
                return;
            }

            BrokerOptionsBuilder? builder = _serviceProvider
                .GetServices<BrokerOptionsBuilder>()
                .FirstOrDefault(b => b.BrokerId == _brokerId);

            if (builder is null)
            {
                _logger.LogError("Cannot recover RabbitMQ: BrokerOptionsBuilder for {BrokerId} was not found.", _brokerId.Id);
                return;
            }

            if (builder.Extension is not RabbitMQExtension extension)
            {
                _logger.LogError(
                    "Cannot recover RabbitMQ: unexpected extension type {ExtensionType}.",
                    builder.Extension?.GetType().FullName);
                return;
            }

            await extension.RecoverAsync(_serviceProvider, builder);
        }
        finally
        {
            await base.HandleChannelShutdownAsync(channel, reason);
        }
    }
}
