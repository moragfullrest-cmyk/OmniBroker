using Microsoft.Extensions.DependencyInjection;

namespace OmniBroker.Infrastructure;

public sealed class MultiBrokerProducer<TMessage> : IProducer<TMessage> where TMessage : IMessage
{
    private readonly IReadOnlyList<IProducer<TMessage>> _producers;

    public MultiBrokerProducer(IServiceProvider services)
    {
        var builders = services.GetServices<BrokerOptionsBuilder>();
        _producers = builders.SelectMany(_ => services.GetKeyedServices<IProducer<TMessage>>(_.BrokerId)).ToList();
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

        var result = true;
        foreach (var producer in _producers)
        {
            result &= await producer.Publish(message, options, cancellationToken);
        }
        return result;
    }
}
