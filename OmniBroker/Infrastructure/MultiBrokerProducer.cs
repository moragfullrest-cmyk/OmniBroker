
using Microsoft.Extensions.DependencyInjection;

namespace OmniBroker.Infrastructure;

public class MultiBrokerProducer<TMessage> : IProducer<TMessage> where TMessage : IMessage
{
    public MultiBrokerProducer(IServiceProvider services)
    {
        var builders = services.GetServices<BrokerOptionsBuilder>();
        Producers = builders.SelectMany(_ => services.GetKeyedServices<IProducer<TMessage>>(_.BrokerId));
    }

    public IEnumerable<IProducer<TMessage>> Producers { get; set; } = [];
    public async Task<bool> Publish(TMessage message)
    {
        var result = true;
        foreach (var producer in Producers)
        {
            result &= await producer.Publish(message);
        }
        return result;
    }
}
