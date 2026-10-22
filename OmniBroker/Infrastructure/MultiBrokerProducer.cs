using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace OmniBroker.Infrastructure;

/// <summary>
/// Publishes to every broker registered for <typeparamref name="TMessage"/>.
/// Returns <see langword="true"/> only when every producer accepts the message.
/// Throws <see cref="InvalidOperationException"/> when none are registered.
/// Throws <see cref="MultiBrokerPublishException"/> when more than one producer is registered and any publish fails or throws.
/// A producer exception is recorded and the remaining brokers are still called.
/// <see cref="OperationCanceledException"/> stops the loop: it is rethrown when nobody has accepted the message, and wrapped as <see cref="Exception.InnerException"/> when somebody already has.
/// Earlier brokers may already have accepted the message; see <see cref="MultiBrokerPublishException.Outcomes"/>.
/// </summary>
public sealed class MultiBrokerProducer<TMessage> : IProducer<TMessage> where TMessage : IMessage
{
    private readonly List<(string BrokerName, IProducer<TMessage> Producer)> _producers;
    private readonly ILogger<MultiBrokerProducer<TMessage>> _logger;

    public MultiBrokerProducer(IServiceProvider services)
    {
        IEnumerable<BrokerOptionsBuilder> builders = services.GetServices<BrokerOptionsBuilder>();
        _producers = [.. builders.SelectMany(builder =>
            services.GetKeyedServices<IProducer<TMessage>>(builder.BrokerName)
                .Select(producer => (builder.BrokerName, producer)))];
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

        if (_producers.Count == 1)
        {
            (string brokerId, IProducer<TMessage> producer) = _producers[0];
            bool published = await producer.Publish(message, options, cancellationToken);
            if (!published)
                LogFailure(brokerId, producer);
            return published;
        }

        var outcomes = new List<BrokerPublishOutcome>(_producers.Count);
        bool anyFailed = false;
        foreach ((string brokerId, IProducer<TMessage> producer) in _producers)
        {
            string producerType = producer.GetType().Name;
            bool published;
            try
            {
                published = await producer.Publish(message, options, cancellationToken);
            }
            catch (OperationCanceledException ex)
            {
                outcomes.Add(new BrokerPublishOutcome(brokerId, producerType, false) { Error = ex });
                if (outcomes.Any(outcome => outcome.Succeeded))
                    throw new MultiBrokerPublishException(typeof(TMessage), outcomes, ex);

                throw;
            }
            catch (Exception ex)
            {
                outcomes.Add(new BrokerPublishOutcome(brokerId, producerType, false) { Error = ex });
                anyFailed = true;
                LogFailure(brokerId, producer, ex);
                continue;
            }

            outcomes.Add(new BrokerPublishOutcome(brokerId, producerType, published));
            if (!published)
            {
                anyFailed = true;
                LogFailure(brokerId, producer);
            }
        }

        if (anyFailed)
            throw new MultiBrokerPublishException(typeof(TMessage), outcomes);

        return true;
    }

    private void LogFailure(string brokerId, IProducer<TMessage> producer, Exception? error = null)
    {
        _logger.LogWarning(
            error,
            "Producer {ProducerType} on broker {BrokerName} failed to publish {MessageType}",
            producer.GetType().Name,
            brokerId,
            typeof(TMessage).Name);
    }
}
