using OmniBroker.Infrastructure;

namespace OmniBroker;

public interface IProducer<TMessage> where TMessage : IMessage
{
    /// <summary>
    /// Отправить сообщение в брокер
    /// </summary>
    Task<bool> Publish(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default);
}
