using OmniBroker.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace OmniBroker;

public interface IBrokerExtension
{
    public BrokerId BrokerId { get; set; }
    Task SetupInfrastructure(IServiceCollection services, BrokerOptionsBuilder builder);
    Task SetupProducers(IServiceCollection services, BrokerOptionsBuilder builder);
    Task SetupConsumers(IServiceCollection services, BrokerOptionsBuilder builder);
    Task SetupRpc(IServiceCollection services, BrokerOptionsBuilder builder);
    Task StartConsumers(IServiceProvider services, BrokerOptionsBuilder builder);
    Task StartInfrastructure(IServiceProvider serviceProvider, BrokerOptionsBuilder builder);
}
