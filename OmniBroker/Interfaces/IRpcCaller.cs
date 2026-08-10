namespace OmniBroker.Interfaces;

/// <summary>
/// Вызов удалённой процедуры через брокер
/// </summary>
public interface IRpcCaller<TInputMessage, TOutputMessage>
    where TInputMessage : IMessage
    where TOutputMessage : IMessage
{
    /// <summary>
    /// Отправить запрос и дождаться ответа
    /// </summary>
    Task<TOutputMessage> Call(TInputMessage input, CancellationToken cancellationToken = default);
}
