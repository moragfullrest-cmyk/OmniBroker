namespace OmniBroker.Infrastructure;

/// <summary>
/// Параметры публикации сообщения
/// </summary>
public sealed class PublishOptions
{
    /// <summary>
    /// Идентификатор корреляции
    /// </summary>
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Адрес ответа (Reply-To)
    /// </summary>
    public string? ReplyTo { get; init; }

    /// <summary>
    /// Явное имя назначения (exchange/topic)
    /// </summary>
    public string? Destination { get; init; }
}
