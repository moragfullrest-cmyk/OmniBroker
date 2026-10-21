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
    private readonly string _brokerId;
    private readonly RabbitMQSettings _settings;

    /// <summary>
    /// Pause before nack with requeue, so a failing message does not immediately occupy the channel again.
    /// Dead-letter nacks do not wait.
    /// </summary>
    internal TimeSpan RequeueDelay { get; set; } = TimeSpan.FromSeconds(1);

    public RabbitMQBasicConsumer(
        IServiceProvider serviceProvider,
        IChannel channel,
        string id,
        ILogger<RabbitMQBasicConsumer> logger) : base(channel)
    {
        _serviceProvider = serviceProvider;
        INameResolver nameResolver = _serviceProvider.GetRequiredKeyedService<INameResolver>(id);
        _channel = channel;
        _handlers = _serviceProvider.GetKeyedServices<HandlerWrapper>(id).ToDictionary(_ => nameResolver.ResolveOutboundName(_.MessageType));
        _brokerId = id;
        _logger = logger;
        _settings = _serviceProvider.GetRequiredKeyedService<RabbitMQSettings>(id);
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
            message.Tag = routingKey;
            var context = new MessageContext(
                CurrentBrokerName: _brokerId,
                ReplyInfo: string.IsNullOrEmpty(properties.ReplyTo)
                    ? null
                    : new RabbitMQReplyInfo { ReplyTo = properties.ReplyTo },
                CancellationToken: cancellationToken);
            HandleResult result = HandleResult.Ack;
            foreach (Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> _delegate in handler.Handlers)
            {
                result = await _delegate(scope.ServiceProvider, message, context);
                if (result == HandleResult.Retry)
                    break;
            }

            if (result == HandleResult.Ack)
            {
                await _channel.BasicAckAsync(deliveryTag, multiple: false, cancellationToken);
            }
            else
            {
                _logger.LogWarning(
                    "Handler returned Retry; nacking delivery {DeliveryTag} (exchange={Exchange}, routingKey={RoutingKey})",
                    deliveryTag,
                    exchange,
                    routingKey);
                await NackAsync(deliveryTag, RequeueOnFailure, cancellationToken);
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
            await NackAsync(deliveryTag, RequeueOnFailure, cancellationToken);
        }
    }

    private bool RequeueOnFailure => string.IsNullOrWhiteSpace(_settings.DeadLetterExchange);

    private async Task NackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken)
    {
        if (requeue)
        {
            try
            {
                await Task.Delay(RequeueDelay, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
        }

        await _channel.BasicNackAsync(deliveryTag, multiple: false, requeue: requeue, cancellationToken);
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

            if (runtime.StoppingToken.IsCancellationRequested)
            {
                _logger.LogDebug("Skipping recover: broker host is stopping.");
                return;
            }

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
                .FirstOrDefault(b => b.BrokerName == _brokerId);

            if (builder is null)
            {
                _logger.LogError("Cannot recover RabbitMQ: BrokerOptionsBuilder for {BrokerName} was not found.", _brokerId);
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
