using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace OmniBroker.RabbitMQ.ServiceSetup;

internal sealed class RabbitMqBrokerHostedService(IServiceProvider services, string brokerId) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        BrokerOptionsBuilder builder = services.GetRequiredKeyedService<BrokerOptionsBuilder>(brokerId);
        IBrokerExtension extension = builder.Extension!;
        await extension.StartInfrastructure(services, builder);
        await extension.StartConsumers(services, builder);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        RabbitMqRuntime runtime = services.GetRequiredKeyedService<RabbitMqRuntime>(brokerId);
        return runtime.ResetConnectionAsync();
    }
}
