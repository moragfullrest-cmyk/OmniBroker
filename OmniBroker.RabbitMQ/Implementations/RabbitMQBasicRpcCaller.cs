using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.ServiceSetup;

namespace OmniBroker.RabbitMQ.Implementations;

internal sealed class RabbitMQBasicRpcCaller<TInputMessage, TOutputMessage> : IRpcCaller<TInputMessage, TOutputMessage>
    where TInputMessage : IMessage
    where TOutputMessage : IMessage
{
    private readonly IProducer<TInputMessage> _producer;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<IMessage>> _awaitedOperations;
    private readonly BrokerOptionsBuilder _builder;
    private readonly INameResolver _nameResolver;
    private readonly RabbitMQSettings _settings;

    public RabbitMQBasicRpcCaller(IServiceProvider _serviceProvider, BrokerId id)
    {
        _builder = _serviceProvider.GetServices<BrokerOptionsBuilder>().First(_ => _.BrokerId == id);
        _producer = _serviceProvider.GetRequiredKeyedService<IProducer<TInputMessage>>(id);
        _awaitedOperations = _serviceProvider.GetRequiredKeyedService<ConcurrentDictionary<string, TaskCompletionSource<IMessage>>>(id);
        _nameResolver = _serviceProvider.GetRequiredKeyedService<INameResolver>(id);
        _settings = _serviceProvider.GetRequiredKeyedService<RabbitMQSettings>(id);
    }

    public async Task<TOutputMessage> Call(TInputMessage input, CancellationToken cancellationToken = default)
    {
        input.Tag = typeof(TInputMessage).Name;
        var correlationId = Guid.NewGuid().ToString();

        var tcs = new TaskCompletionSource<IMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_awaitedOperations.TryAdd(correlationId, tcs))
        {
            throw new InvalidOperationException($"Pending operation for correlation id '{correlationId}' already exists.");
        }

        try
        {
            var published = await _producer.Publish(input, new PublishOptions
            (
                CorrelationId: correlationId,
                ReplyTo: ((RabbitMQExtension)_builder.Extension!).ReplyQueueName,
                Destination: _nameResolver.ResolveOutboundName(typeof(TInputMessage))
            ), cancellationToken);

            if (!published)
            {
                throw new InvalidOperationException("Failed to publish RPC request.");
            }

            using var timeoutCts = new CancellationTokenSource(_settings.RpcTimeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);
            try
            {
                IMessage result = await tcs.Task.WaitAsync(linkedCts.Token);
                return (TOutputMessage)result;
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                tcs.TrySetCanceled();
                throw new TimeoutException("The operation has timed out.");
            }
            catch (OperationCanceledException)
            {
                tcs.TrySetCanceled();
                throw;
            }
        }
        finally
        {
            _awaitedOperations.TryRemove(correlationId, out _);
        }
    }
}
