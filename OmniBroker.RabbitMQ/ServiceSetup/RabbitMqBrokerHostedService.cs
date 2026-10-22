using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace OmniBroker.RabbitMQ.ServiceSetup;

internal sealed class RabbitMqBrokerHostedService(IServiceProvider services, string brokerId) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        BrokerOptionsBuilder builder = services.GetRequiredKeyedService<BrokerOptionsBuilder>(brokerId);
        IBrokerExtension extension = builder.Extension!;
        await extension.ValidateRemoteBrokerState(services, builder, cancellationToken);
        await extension.StartInfrastructure(services, builder, cancellationToken);
        await extension.StartConsumers(services, builder, cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        RabbitMqRuntime runtime = services.GetRequiredKeyedService<RabbitMqRuntime>(brokerId);
        runtime.SignalStop();
        await runtime.ResetConnectionAsync(cancellationToken);
    }
}
