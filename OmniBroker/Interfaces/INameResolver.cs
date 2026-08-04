namespace OmniBroker.Interfaces;

public interface INameResolver
{
    string ResolveOutboundName(Type messageType);
    string ResolveInboundName(Type messageType);

}
