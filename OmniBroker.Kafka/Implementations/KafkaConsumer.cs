using System.Linq.Expressions;
using System.Reflection;
using Confluent.Kafka;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.ServiceSetup;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace OmniBroker.Kafka.Implementations;

internal class KafkaConsumer : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly Dictionary<string, HandlerWrapper> _handlers;
    private readonly IConsumer<string, byte[]> _consumer;
    private readonly ILogger _logger;
    private readonly Dictionary<string, Func<IMessage>> _messageActivators = new Dictionary<string, Func<IMessage>>();
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

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        _handlers.Keys.ToList().ForEach(h =>
        {
            _consumer.Subscribe(h);
        });

        return base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        while (stoppingToken.IsCancellationRequested == false)
        {
            try
            {
                ConsumeResult<string, byte[]>? receivedMessage = _consumer?.Consume(stoppingToken);
                if (receivedMessage?.Message?.Value == null || receivedMessage.Message.Value.Length == 0)
                    continue;

                if (_handlers.TryGetValue(receivedMessage.Topic, out HandlerWrapper? handler))
                {
                    var scope = _serviceProvider.CreateScope();
                    IMessage message = _messageActivators[receivedMessage.Topic]();
                    if (message != null)
                    {
                        message.Body = receivedMessage.Message.Value;
                        bool result = true;
                        foreach (Func<IServiceProvider, IMessage, MessageContext, Task<bool>> _delegate in handler.Handlers)
                        {
                            result &= await _delegate(scope.ServiceProvider, message, new MessageContext
                            {
                                CurrentBrokerId = _brokerId
                            });
                        }
                        if (result)
                            _consumer?.Commit(receivedMessage);
                    }
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Kafka consume error: {Reason}", ex.Error.Reason);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error processing Kafka message");
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_consumer != null)
        {
            _logger.LogInformation("Closing Kafka consumer...");
            _consumer.Close();
            _consumer.Dispose();
        }

        await base.StopAsync(cancellationToken);
    }
}
