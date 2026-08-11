using Microsoft.Extensions.DependencyInjection;

namespace OmniBroker.Infrastructure;

public sealed class MultiBrokerProducer<TMessage> : IProducer<TMessage> where TMessage : IMessage
{
    private readonly List<IProducer<TMessage>> _producers;

    public MultiBrokerProducer(IServiceProvider services)
    {
        IEnumerable<BrokerOptionsBuilder> builders = services.GetServices<BrokerOptionsBuilder>();
        _producers = [.. builders.SelectMany(_ => services.GetKeyedServices<IProducer<TMessage>>(_.BrokerId))];
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
            result &= await producer.Publish(message, options, cancellationToken);
        }
        return result;
    }
}
