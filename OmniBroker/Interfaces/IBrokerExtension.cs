using Microsoft.Extensions.DependencyInjection;

namespace OmniBroker;

/// <summary>
/// Broker transport extension (infrastructure, producers, consumers, RPC)
/// </summary>
public interface IBrokerExtension
{
    /// <summary>
    /// Broker instance identifier used as the keyed DI key
    /// </summary>
    string BrokerId { get; }

    /// <summary>
    /// Whether the transport supports RPC
    /// </summary>
    bool SupportsRpc { get; }

    /// <summary>
    /// Register broker infrastructure in DI
    /// </summary>
    Task SetupInfrastructure(IServiceCollection services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Register producers in DI
    /// </summary>
    Task SetupProducers(IServiceCollection services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Register consumers in DI
    /// </summary>
    Task SetupConsumers(IServiceCollection services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Register RPC components in DI
    /// </summary>
    Task SetupRpc(IServiceCollection services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Start message consumption
    /// </summary>
    Task StartConsumers(IServiceProvider services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Start broker infrastructure
    /// </summary>
    Task StartInfrastructure(IServiceProvider serviceProvider, BrokerOptionsBuilder builder);
}
