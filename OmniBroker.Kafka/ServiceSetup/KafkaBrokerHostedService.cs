using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace OmniBroker.Kafka.ServiceSetup;

internal sealed class KafkaBrokerHostedService(IServiceProvider services, string brokerId) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        BrokerOptionsBuilder builder = services.GetRequiredKeyedService<BrokerOptionsBuilder>(brokerId);
        return builder.Extension!.ValidateRemoteBrokerState(services, builder);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
