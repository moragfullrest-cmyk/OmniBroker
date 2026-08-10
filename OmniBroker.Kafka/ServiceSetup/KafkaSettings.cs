using Confluent.Kafka;

namespace OmniBroker.Kafka.ServiceSetup;

/// <summary>
/// Настройки подключения к Kafka
/// </summary>
public sealed class KafkaSettings
{
    /// <summary>
    /// Список bootstrap-серверов
    /// </summary>
    public required string Hosts { get; init; }

    /// <summary>
    /// Идентификатор клиента
    /// </summary>
    public string? ClientId { get; init; }

    /// <summary>
    /// Идентификатор consumer group
    /// </summary>
    public string? GroupId { get; init; }

    /// <summary>
    /// Стратегия сброса offset при отсутствии сохранённой позиции
    /// </summary>
    public AutoOffsetReset AutoOffsetReset { get; init; } = AutoOffsetReset.Earliest;

    /// <summary>
    /// Протокол безопасности подключения
    /// </summary>
    public SecurityProtocol? SecurityProtocol { get; init; }

    /// <summary>
    /// Имя пользователя SASL
    /// </summary>
    public string? SaslUsername { get; init; }

    /// <summary>
    /// Пароль SASL
    /// </summary>
    public string? SaslPassword { get; init; }

    /// <summary>
    /// Механизм SASL
    /// </summary>
    public SaslMechanism? SaslMechanism { get; init; }
}
