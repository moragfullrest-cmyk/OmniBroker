using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;

namespace OmniBroker.Kafka.Implementations;

internal sealed class KafkaConsumer : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Dictionary<string, HandlerWrapper> _handlers;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger _logger;
    private readonly BrokerId _brokerId;

    public KafkaConsumer(
        IServiceProvider serviceProvider,
        BrokerId id,
        ILogger<KafkaConsumer> logger,
        INameResolver nameResolver)
    {
        _serviceProvider = serviceProvider;
        _handlers = _serviceProvider.GetKeyedServices<HandlerWrapper>(id).ToDictionary(_ => nameResolver.ResolveInboundName(_.MessageType));
        _consumer = _serviceProvider.GetRequiredKeyedService<IConsumer<string, byte[]>>(id);
        _logger = logger;
        _brokerId = id;
    }

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        if (_handlers.Count > 0)
        {
            _consumer.Subscribe(_handlers.Keys);
        }

        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        while (stoppingToken.IsCancellationRequested == false)
        {
            ConsumeResult<string, byte[]>? receivedMessage = null;
            try
            {
                receivedMessage = _consumer.Consume(stoppingToken);
                if (receivedMessage is null)
                    continue;
                if (receivedMessage.Message?.Value == null || receivedMessage.Message.Value.Length == 0)
                {
                    _consumer.Commit(receivedMessage);
                    continue;
                }

                if (!_handlers.TryGetValue(receivedMessage.Topic, out HandlerWrapper? handler))
                {
                    _logger.LogWarning(
                        "No handler for topic {Topic}; committing and skipping. Offset={Offset}",
                        receivedMessage.Topic,
                        receivedMessage.Offset);
                    _consumer.Commit(receivedMessage);
                    continue;
                }

                using var scope = _serviceProvider.CreateScope();
                IMessage message = handler.CreateMessage();
                message.Body = receivedMessage.Message.Value;
                message.CorrelationId = ReadCorrelationId(receivedMessage.Message.Headers) ?? message.CorrelationId;

                bool result = true;
                foreach (Func<IServiceProvider, IMessage, MessageContext, Task<bool>> handlerDelegate in handler.Handlers)
                {
                    result &= await handlerDelegate(scope.ServiceProvider, message, new MessageContext
                    (
                        CurrentBrokerId: _brokerId,
                        null
                    ));
                }

                if (!result)
                {
                    _logger.LogWarning(
                        "Handler returned false; committing and skipping. Topic={Topic}, Offset={Offset}",
                        receivedMessage.Topic,
                        receivedMessage.Offset);
                }

                _consumer.Commit(receivedMessage);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error processing Kafka message");
                if (receivedMessage is not null)
                {
                    try
                    {
                        _consumer.Commit(receivedMessage);
                    }
                    catch (Exception commitEx)
                    {
                        _logger.LogError(commitEx, "Failed to commit Kafka offset after handler error");
                    }
                }
            }
        }
    }

    private static string? ReadCorrelationId(Headers? headers)
    {
        if (headers is null)
            return null;

        IHeader? header = headers.FirstOrDefault(h => h.Key == KafkaMessageHeaders.CorrelationId);
        if (header is null)
            return null;

        return Encoding.UTF8.GetString(header.GetValueBytes());
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await base.StopAsync(cancellationToken);
        }
        finally
        {
            _logger.LogInformation("Closing Kafka consumer...");
            _consumer.Close();
            // Do NOT Dispose here — IConsumer is keyed singleton owned by DI
        }
    }
}
