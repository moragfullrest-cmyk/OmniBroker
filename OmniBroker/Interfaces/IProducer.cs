using OmniBroker.Infrastructure;

namespace OmniBroker;

public interface IProducer<TMessage> where TMessage : IMessage
{
    /// <summary>
    /// Publish a message to the broker
    /// </summary>
    Task<bool> Publish(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default);
}
