using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace OmniBroker.Infrastructure;

public sealed class MultiBrokerProducer<TMessage> : IProducer<TMessage> where TMessage : IMessage
{
    private readonly List<IProducer<TMessage>> _producers;
    private readonly ILogger<MultiBrokerProducer<TMessage>> _logger;

    public MultiBrokerProducer(IServiceProvider services)
    {
        IEnumerable<BrokerOptionsBuilder> builders = services.GetServices<BrokerOptionsBuilder>();
        _producers = [.. builders.SelectMany(_ => services.GetKeyedServices<IProducer<TMessage>>(_.BrokerId))];
        _logger = services.GetService<ILogger<MultiBrokerProducer<TMessage>>>()
            ?? NullLogger<MultiBrokerProducer<TMessage>>.Instance;
    }

    public async Task<bool> Publish(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (_producers.Count == 0)
        {
            return false;
        }

        if (_producers.Count > 1 && options?.Destination is not null)
        {
            throw new InvalidOperationException(
                "PublishOptions.Destination cannot be used with MultiBrokerProducer when more than one broker is registered for this message type.");
        }

        bool result = true;
        foreach (IProducer<TMessage> producer in _producers)
        {
            bool published = await producer.Publish(message, options, cancellationToken);
            if (!published)
            {
                _logger.LogWarning(
                    "Producer {ProducerType} failed to publish {MessageType}",
                    producer.GetType().Name,
                    typeof(TMessage).Name);
                result = false;
            }
        }
        return result;
    }
}
