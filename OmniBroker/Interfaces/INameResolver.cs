namespace OmniBroker.Interfaces;

/// <summary>
/// Резолвер имён входящих и исходящих каналов сообщений
/// </summary>
public interface INameResolver
{
    /// <summary>
    /// Получить имя исходящего канала для типа сообщения
    /// </summary>
    string ResolveOutboundName(Type messageType);

    /// <summary>
    /// Получить имя входящего канала для типа сообщения
    /// </summary>
    string ResolveInboundName(Type messageType);
}
