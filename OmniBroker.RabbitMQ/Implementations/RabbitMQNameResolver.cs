using OmniBroker.Interfaces;

namespace OmniBroker.RabbitMQ.Implementations;

/// <summary>
/// Resolves RabbitMQ channel names.
/// Inbound queues are "{namePrefix}_{message type}". Outbound names are the message type name.
/// UseRabbitMq passes the broker name when NameResolver is not set.
/// </summary>
/// <param name="namePrefix">Inbound queue prefix.</param>
public sealed class RabbitMQNameResolver(string namePrefix) : INameResolver
{
    public string ResolveInboundName(Type messageType)
    {
        if (messageType.IsAssignableTo(typeof(IMessage)) == false)
            throw new ArgumentException($"{messageType} has to implement {nameof(IMessage)}");
        return $"{namePrefix}_{messageType.Name}";
    }

    public string ResolveOutboundName(Type messageType)
    {
        if (messageType.IsAssignableTo(typeof(IMessage)) == false)
            throw new ArgumentException($"{messageType} has to implement {nameof(IMessage)}");
        return messageType.Name;
    }
}
