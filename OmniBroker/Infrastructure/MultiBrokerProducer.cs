using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace OmniBroker.Infrastructure;

/// <summary>
/// Publishes to every broker registered for <typeparamref name="TMessage"/>.
/// Returns <see langword="true"/> only when every producer accepts the message.
/// Throws <see cref="InvalidOperationException"/> when none are registered.
/// Throws <see cref="MultiBrokerPublishException"/> when more than one producer is registered and any publish fails.
/// Earlier brokers may already have accepted the message; see <see cref="MultiBrokerPublishException.Outcomes"/>.
/// </summary>
public sealed class MultiBrokerProducer<TMessage> : IProducer<TMessage> where TMessage : IMessage
{
    private readonly List<(string BrokerId, IProducer<TMessage> Producer)> _producers;
    private readonly ILogger<MultiBrokerProducer<TMessage>> _logger;

    public MultiBrokerProducer(IServiceProvider services)
    {
        IEnumerable<BrokerOptionsBuilder> builders = services.GetServices<BrokerOptionsBuilder>();
        _producers = [.. builders.SelectMany(builder =>
            services.GetKeyedServices<IProducer<TMessage>>(builder.BrokerId)
                .Select(producer => (builder.BrokerId, producer)))];
        _logger = services.GetService<ILogger<MultiBrokerProducer<TMessage>>>()
            ?? NullLogger<MultiBrokerProducer<TMessage>>.Instance;
    }

    public async Task<bool> Publish(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (_producers.Count == 0)
        {
            throw new InvalidOperationException(
                $"No producers are registered for message type {typeof(TMessage).Name}.");
        }

        if (_producers.Count > 1 && options?.Destination is not null)
        {
            throw new InvalidOperationException(
                "PublishOptions.Destination cannot be used with MultiBrokerProducer when more than one broker is registered for this message type.");
        }

        var outcomes = new List<BrokerPublishOutcome>(_producers.Count);
        foreach ((string brokerId, IProducer<TMessage> producer) in _producers)
        {
            bool published = await producer.Publish(message, options, cancellationToken);
            outcomes.Add(new BrokerPublishOutcome(brokerId, producer.GetType().Name, published));
            if (!published)
            {
                _logger.LogWarning(
                    "Producer {ProducerType} on broker {BrokerId} failed to publish {MessageType}",
                    producer.GetType().Name,
                    brokerId,
                    typeof(TMessage).Name);
            }
        }

        if (_producers.Count > 1 && outcomes.Any(outcome => !outcome.Succeeded))
            throw new MultiBrokerPublishException(typeof(TMessage), outcomes);

        return outcomes.All(outcome => outcome.Succeeded);
    }
}
