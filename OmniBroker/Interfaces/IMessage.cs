namespace OmniBroker;

public interface IMessage
{
    /// <summary>
    /// Routing keys для bind очереди. Переопределите на типе сообщения при необходимости.
    /// </summary>
    string[] GetAcceptableTags() => [""];

    /// <summary>
    /// Тело сообщения
    /// </summary>
    public byte[] Body { get; set; }

    /// <summary>
    /// Тэг конкретного сообщения
    /// </summary>
    public string Tag { get; set; }

    /// <summary>
    /// Ид корреляции сообщений
    /// </summary>
    public string CorrelationId { get; set; }
}
