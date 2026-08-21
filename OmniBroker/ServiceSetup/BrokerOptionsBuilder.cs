using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;

namespace OmniBroker;

/// <summary>
/// Broker configuration options builder
/// </summary>
public sealed class BrokerOptionsBuilder
{
    internal Dictionary<Type, Type> RpcCallers { get; set; } = [];
    internal Dictionary<Type, Delegate> RpcReceivers { get; set; } = [];
    internal HashSet<Type> Producables { get; set; } = [];
    internal Dictionary<Type, List<Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>>>> Consumables { get; set; } = [];
    internal IBrokerExtension Extension { get; set; }

    /// <summary>
    /// Consuming side name for the broker
    /// </summary>
    public string SetupName { get; set; } = Guid.NewGuid().ToString();

    /// <summary>
    /// Topic/queue name resolver
    /// </summary>
    public INameResolver? NameResolver { get; set; }

    internal string BrokerId { get; set; } = Guid.NewGuid().ToString();
}
