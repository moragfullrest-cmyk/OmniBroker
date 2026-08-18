using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.ServiceSetup;

namespace OmniBroker.Kafka.Implementations;

internal sealed class KafkaConsumer : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Dictionary<string, HandlerWrapper> _handlers;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly IProducer<string, byte[]>? _producer;
    private readonly ILogger _logger;
    private readonly BrokerId _brokerId;
    private readonly string? _deadLetterTopic;

    public KafkaConsumer(
        IServiceProvider serviceProvider,
        BrokerId id,
        ILogger<KafkaConsumer> logger,
        INameResolver nameResolver,
        KafkaSettings? settings = null)
    {
        _serviceProvider = serviceProvider;
        _handlers = _serviceProvider.GetKeyedServices<HandlerWrapper>(id).ToDictionary(_ => nameResolver.ResolveInboundName(_.MessageType));
        _consumer = _serviceProvider.GetRequiredKeyedService<IConsumer<string, byte[]>>(id);
        _logger = logger;
        _brokerId = id;
        _deadLetterTopic = settings?.DeadLetterTopic;
        if (!string.IsNullOrWhiteSpace(_deadLetterTopic))
            _producer = _serviceProvider.GetRequiredKeyedService<IProducer<string, byte[]>>(id);
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

                var context = new MessageContext(
                    CurrentBrokerId: _brokerId,
                    ReplyInfo: null,
                    CancellationToken: stoppingToken);
                HandleResult result = HandleResult.Ack;
                foreach (Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> handlerDelegate in handler.Handlers)
                {
                    if (await handlerDelegate(scope.ServiceProvider, message, context) == HandleResult.Retry)
                        result = HandleResult.Retry;
                }

                if (result == HandleResult.Retry)
                {
                    _logger.LogWarning(
                        "Handler returned Retry; committing and skipping. Topic={Topic}, Offset={Offset}",
                        receivedMessage.Topic,
                        receivedMessage.Offset);
                    await TryPublishDeadLetterAsync(receivedMessage, "Handler returned Retry", stoppingToken);
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
                    await TryPublishDeadLetterAsync(receivedMessage, ex.Message, stoppingToken);
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

    private async Task TryPublishDeadLetterAsync(
        ConsumeResult<string, byte[]> receivedMessage,
        string? reason,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_deadLetterTopic) || _producer is null)
            return;

        try
        {
            var headers = new Headers
            {
                { KafkaMessageHeaders.SourceTopic, Encoding.UTF8.GetBytes(receivedMessage.Topic) }
            };

            string? correlationId = ReadCorrelationId(receivedMessage.Message.Headers);
            if (!string.IsNullOrEmpty(correlationId))
                headers.Add(KafkaMessageHeaders.CorrelationId, Encoding.UTF8.GetBytes(correlationId));

            if (!string.IsNullOrEmpty(reason))
                headers.Add(KafkaMessageHeaders.Error, Encoding.UTF8.GetBytes(reason));

            await _producer.ProduceAsync(
                _deadLetterTopic,
                new Message<string, byte[]>
                {
                    Key = receivedMessage.Message.Key,
                    Value = receivedMessage.Message.Value,
                    Headers = headers
                },
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to publish Kafka message to dead-letter topic {DeadLetterTopic}",
                _deadLetterTopic);
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

            IProducer<string, byte[]>? producer = _serviceProvider.GetKeyedService<IProducer<string, byte[]>>(_brokerId);
            if (producer is not null)
            {
                producer.Flush(cancellationToken);
                // Do NOT Dispose here — IProducer is keyed singleton owned by DI
            }
        }
    }
}
