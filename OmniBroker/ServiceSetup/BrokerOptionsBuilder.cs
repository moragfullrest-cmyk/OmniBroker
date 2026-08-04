using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;

namespace OmniBroker;

public class BrokerOptionsBuilder
{
    internal Dictionary<Type, Type> RpcCallers { get; set; } = [];
    internal Dictionary<Type, Delegate> RpcReceivers { get; set; } = [];
    internal HashSet<Type> Producables { get; set; } = [];
    internal Dictionary<Type, List<Func<IServiceProvider, IMessage, MessageContext, Task<bool>>>> Consumables { get; set; } = [];
    internal IBrokerExtension Extension { get; set; }

    /// <summary>
    /// Имя потребляющей стороны брокера
    /// </summary>
    public string SetupName { get; set; } = Guid.NewGuid().ToString();
    public INameResolver? NameResolver { get; set; }
    internal BrokerId BrokerId { get; set; } = new BrokerId();
}
