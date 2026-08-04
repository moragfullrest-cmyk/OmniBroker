using OmniBroker.Interfaces;

namespace OmniBroker.RabbitMQ.Implementations;

internal class RabbitMQNameResolver(string NamePrefix) : INameResolver
{
    public string ResolveInboundName(Type messageType)
    {
        if (messageType.IsAssignableTo(typeof(IMessage)) == false)
            throw new ArgumentException($"{messageType} has to implement {nameof(IMessage)}");
        return $"{NamePrefix}_{messageType.Name}";
    }
    public string ResolveOutboundName(Type messageType)
    {
        if (messageType.IsAssignableTo(typeof(IMessage)) == false)
            throw new ArgumentException($"{messageType} has to implement {nameof(IMessage)}");
        return messageType.Name;
    }
}
