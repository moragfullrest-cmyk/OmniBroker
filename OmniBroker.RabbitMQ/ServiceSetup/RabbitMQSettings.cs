namespace OmniBroker.RabbitMQ.ServiceSetup;

/// <summary>
/// Настройки подключения к RabbitMQ
/// </summary>
public sealed class RabbitMQSettings
{
    /// <summary>
    /// Хост брокера
    /// </summary>
    public required string HostName { get; init; }

    /// <summary>
    /// Имя пользователя
    /// </summary>
    public required string UserName { get; init; }

    /// <summary>
    /// Пароль
    /// </summary>
    public required string Password { get; init; }

    /// <summary>
    /// Виртуальный хост
    /// </summary>
    public string VirtualHost { get; init; } = "/";

    /// <summary>
    /// Порт подключения
    /// </summary>
    public int Port { get; init; } = 5672;

    /// <summary>
    /// Использовать TLS
    /// </summary>
    public bool UseTls { get; init; }

    /// <summary>
    /// Таймаут ожидания RPC-ответа
    /// </summary>
    public TimeSpan RpcTimeout { get; init; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Максимальный размер пула каналов
    /// </summary>
    public int MaxChannelPoolSize { get; init; } = 32;
}
