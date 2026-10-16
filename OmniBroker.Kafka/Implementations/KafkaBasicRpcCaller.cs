using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.ServiceSetup;

namespace OmniBroker.Kafka.Implementations;

internal sealed class KafkaBasicRpcCaller<TInputMessage, TOutputMessage> : IRpcCaller<TInputMessage, TOutputMessage>
    where TInputMessage : IMessage
    where TOutputMessage : IMessage
{
    private static readonly Func<IMessage> CreateOutputMessage =
        HandlerWrapper.BuildCreateMessage(typeof(TOutputMessage));

    private readonly IProducer<TInputMessage> _producer;
    private readonly ConcurrentDictionary<string, KafkaRpcWait> _awaitedOperations;
    private readonly INameResolver _nameResolver;
    private readonly KafkaSettings _settings;
    private readonly KafkaRpcReplyLease _lease;

    public KafkaBasicRpcCaller(IServiceProvider serviceProvider, string id)
    {
        _producer = serviceProvider.GetRequiredKeyedService<IProducer<TInputMessage>>(id);
        _awaitedOperations = serviceProvider.GetRequiredKeyedService<ConcurrentDictionary<string, KafkaRpcWait>>(id);
        _nameResolver = serviceProvider.GetRequiredKeyedService<INameResolver>(id);
        _settings = serviceProvider.GetRequiredKeyedService<KafkaSettings>(id);
        _lease = serviceProvider.GetRequiredKeyedService<KafkaRpcReplyLease>(id);
    }

    public async Task<TOutputMessage> Call(TInputMessage input, CancellationToken cancellationToken = default)
    {
        input.Tag = typeof(TInputMessage).Name;
        await _lease.Ready.WaitAsync(cancellationToken);
        int partitionId = _lease.PartitionId
            ?? throw new InvalidOperationException("Kafka RPC reply partition is not claimed.");
        if (string.IsNullOrWhiteSpace(_settings.RpcReplyTopic))
            throw new InvalidOperationException(KafkaRpcReplyStrings.MissingTopicMessage());

        var correlationId = Guid.NewGuid().ToString();
        var completion = new TaskCompletionSource<IMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var wait = new KafkaRpcWait(completion, CreateOutputMessage);
        if (!_awaitedOperations.TryAdd(correlationId, wait))
            throw new InvalidOperationException($"Pending operation for correlation id '{correlationId}' already exists.");

        try
        {
            bool published = await _producer.Publish(input, new KafkaPublishOptions(
                CorrelationId: correlationId,
                ReplyTo: _settings.RpcReplyTopic,
                Destination: _nameResolver.ResolveOutboundName(typeof(TInputMessage)),
                ReplyPartition: partitionId), cancellationToken);

            if (!published)
                throw new InvalidOperationException("Failed to publish RPC request.");

            using var timeoutCts = new CancellationTokenSource(_settings.RpcTimeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, cancellationToken);
            try
            {
                IMessage result = await completion.Task.WaitAsync(linkedCts.Token);
                return (TOutputMessage)result;
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
            {
                completion.TrySetCanceled();
                throw new TimeoutException("The operation has timed out.");
            }
            catch (OperationCanceledException)
            {
                completion.TrySetCanceled();
                throw;
            }
        }
        finally
        {
            _awaitedOperations.TryRemove(correlationId, out _);
        }
    }
}
