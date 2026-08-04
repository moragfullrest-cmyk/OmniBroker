namespace OmniBroker;

public interface IMessage
{
    /// <summary>
    /// Набор тэгов которые могут быть у сообщения
    /// </summary>
    public static string[]? AcceptableTags { get; }

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
