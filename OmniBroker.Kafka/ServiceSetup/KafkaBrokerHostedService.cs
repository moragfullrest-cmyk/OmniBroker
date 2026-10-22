using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace OmniBroker.Kafka.ServiceSetup;

internal sealed class KafkaBrokerHostedService(IServiceProvider services, string brokerId) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        BrokerOptionsBuilder builder = services.GetRequiredKeyedService<BrokerOptionsBuilder>(brokerId);
        await builder.Extension!.ValidateRemoteBrokerState(services, builder, cancellationToken);
        await builder.Extension.StartInfrastructure(services, builder, cancellationToken);
        await builder.Extension.StartConsumers(services, builder, cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
