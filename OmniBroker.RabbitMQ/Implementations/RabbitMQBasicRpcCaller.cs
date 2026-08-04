using System.Collections.Concurrent;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.ServiceSetup;
using Microsoft.Extensions.DependencyInjection;

namespace OmniBroker.RabbitMQ.Implementations;

internal class RabbitMQBasicRpcCaller<TInputMessage, TOutputMessage> : IRpcCaller<TInputMessage, TOutputMessage>
    where TInputMessage : IMessage
    where TOutputMessage : IMessage
{
    private readonly IProducer<TInputMessage> _producer;
    private readonly ConcurrentDictionary<string, PendingOperation> _awaitedOperations;
    private readonly BrokerOptionsBuilder _builder;
    private readonly INameResolver _nameResolver;

    public RabbitMQBasicRpcCaller(IServiceProvider _serviceProvider, BrokerId id)
    {
        _builder = _serviceProvider.GetServices<BrokerOptionsBuilder>().First(_ => _.BrokerId == id);
        _producer = _serviceProvider.GetRequiredKeyedService<IProducer<TInputMessage>>(id);
        _awaitedOperations = _serviceProvider.GetRequiredKeyedService<ConcurrentDictionary<string, PendingOperation>>(id);
        _nameResolver = _serviceProvider.GetRequiredKeyedService<INameResolver>(id);
    }

    public async Task<TOutputMessage> Call(TInputMessage input)
    {
        input.Tag = typeof(TInputMessage).Name;
        var localProducer = (RabbitMQBasicProducer<TInputMessage>)_producer;
        localProducer.ExchangeName = _nameResolver.ResolveOutboundName(typeof(TInputMessage));
        var correlationId = Guid.NewGuid().ToString();
        localProducer.CorrelationId = correlationId;
        localProducer.ReplyQueue = ((RabbitMQExtension)_builder.Extension).ReplyQueueName;

        await _producer.Publish(input);
        _awaitedOperations.AddOrUpdate(key: correlationId, addValue: new PendingOperation { CallTime = DateTime.Now, Timedout = false, Result = null }, updateValueFactory: (s, b) => b);

        using (var timeoutCancellationTokenSource = new CancellationTokenSource())
        {
            var task = Task.Run(() =>
            {
                while (timeoutCancellationTokenSource.Token.IsCancellationRequested == false)
                {
                    var result = _awaitedOperations[correlationId];
                    if (result.Result != null)
                    {
                        return (TOutputMessage)result.Result;
                    }
                    Task.Delay(100);
                }
                return default;
            });
            Task completedTask = await Task.WhenAny(task, Task.Delay((int)TimeSpan.FromMinutes(1).TotalMilliseconds, timeoutCancellationTokenSource.Token));
            await timeoutCancellationTokenSource.CancelAsync();
            if (completedTask == task)
            {
                return await task;  // Very important in order to propagate exceptions
            }
            else
            {
                _awaitedOperations.TryUpdate(correlationId, _awaitedOperations[correlationId] with { Timedout = true }, _awaitedOperations[correlationId]);
                throw new TimeoutException("The operation has timed out.");
            }
        }

    }
}
