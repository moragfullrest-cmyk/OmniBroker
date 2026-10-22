using Microsoft.Extensions.DependencyInjection;
using OmniBroker.Interfaces;

namespace OmniBroker;

internal static class BrokerWireNames
{
    internal readonly record struct ReservedName(string Role, string Name);

    internal static void EnsureDistinct(BrokerOptionsBuilder builder, IEnumerable<ReservedName>? reservedNames = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(builder.NameResolver);
        INameResolver resolver = builder.NameResolver;

        var inbound = new Dictionary<string, Type>(StringComparer.Ordinal);
        var outbound = new Dictionary<string, Type>(StringComparer.Ordinal);
        foreach (Type type in EnumerateTypes(builder))
        {
            Track(inbound, resolver.ResolveInboundName(type), type);
            Track(outbound, resolver.ResolveOutboundName(type), type);
        }

        if (reservedNames is null)
            return;

        foreach (ReservedName reserved in reservedNames)
        {
            if (string.IsNullOrWhiteSpace(reserved.Name))
                continue;
            if (inbound.TryGetValue(reserved.Name, out Type? inboundOwner))
                ThrowReserved(reserved, inboundOwner);
            if (outbound.TryGetValue(reserved.Name, out Type? outboundOwner))
                ThrowReserved(reserved, outboundOwner);
        }
    }

    internal static void AddKeyedMessageProducer(
        this IServiceCollection services,
        Type messageType,
        object brokerName,
        Func<IServiceProvider, object?, object> factory)
    {
        Type serviceType = typeof(IProducer<>).MakeGenericType(messageType);
        if (services.Any(descriptor => descriptor.IsKeyedService
            && Equals(descriptor.ServiceKey, brokerName)
            && descriptor.ServiceType == serviceType))
        {
            return;
        }

        services.AddKeyedSingleton(serviceType, brokerName, factory);
    }

    private static IEnumerable<Type> EnumerateTypes(BrokerOptionsBuilder builder)
    {
        foreach (Type type in builder.Producables)
            yield return type;
        foreach (Type type in builder.Consumables.Keys)
            yield return type;
        foreach (KeyValuePair<Type, Type> caller in builder.RpcCallers)
        {
            yield return caller.Key;
            yield return caller.Value;
        }

        foreach (KeyValuePair<Type, Delegate> receiver in builder.RpcReceivers)
        {
            yield return receiver.Key;
            yield return ReceiverReturnType(receiver.Value);
        }
    }

    private static Type ReceiverReturnType(Delegate receiver)
    {
        Type returnType = receiver.Method.ReturnType;
        if (returnType.IsGenericType && returnType.GenericTypeArguments.Length == 1)
            return returnType.GenericTypeArguments[0];

        throw new ArgumentException($"RPC receiver must return Task<T>, but returned {returnType}.");
    }

    private static void Track(Dictionary<string, Type> seen, string name, Type type)
    {
        if (seen.TryGetValue(name, out Type? existing))
        {
            if (existing == type)
                return;

            throw new ArgumentException($"Wire name '{name}' is shared by {existing.FullName} and {type.FullName}.");
        }

        seen.Add(name, type);
    }

    private static void ThrowReserved(ReservedName reserved, Type owner)
        => throw new ArgumentException($"Wire name '{reserved.Name}' is shared by {owner.FullName} and {reserved.Role}.");
}
