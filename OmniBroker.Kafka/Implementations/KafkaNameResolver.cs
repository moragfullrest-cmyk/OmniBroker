using OmniBroker.Interfaces;

namespace OmniBroker.Kafka.Implementations;

internal class KafkaNameResolver : INameResolver
{
    public string ResolveInboundName(Type messageType)
    {
        if (messageType.IsAssignableTo(typeof(IMessage)) == false)
            throw new ArgumentException($"{messageType} has to implement {nameof(IMessage)}");
        return messageType.Name;
    }
    public string ResolveOutboundName(Type messageType)
    {
        {
            if (messageType.IsAssignableTo(typeof(IMessage)) == false)
                throw new ArgumentException($"{messageType} has to implement {nameof(IMessage)}");
            return messageType.Name;
        }
    }
}
