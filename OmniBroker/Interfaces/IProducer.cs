namespace OmniBroker;

public interface IProducer<TMessage> where TMessage : IMessage
{
    /// <summary>
    /// Отправить сообщение в брокер
    /// </summary>
    public Task<bool> Publish(TMessage message);
}
