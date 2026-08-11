namespace OmniBroker.Interfaces;

/// <summary>
/// Resolver for inbound and outbound message channel names
/// </summary>
public interface INameResolver
{
    /// <summary>
    /// Get the outbound channel name for a message type
    /// </summary>
    string ResolveOutboundName(Type messageType);

    /// <summary>
    /// Get the inbound channel name for a message type
    /// </summary>
    string ResolveInboundName(Type messageType);
}
