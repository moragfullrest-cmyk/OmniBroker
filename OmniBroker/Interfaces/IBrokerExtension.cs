using OmniBroker.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace OmniBroker;

/// <summary>
/// Расширение транспорта брокера (инфраструктура, продюсеры, консьюмеры, RPC)
/// </summary>
public interface IBrokerExtension
{
    /// <summary>
    /// Идентификатор экземпляра брокера
    /// </summary>
    public BrokerId BrokerId { get; set; }

    /// <summary>
    /// Поддерживает ли транспорт RPC-обмен
    /// </summary>
    bool SupportsRpc { get; }

    /// <summary>
    /// Зарегистрировать инфраструктуру брокера в DI
    /// </summary>
    Task SetupInfrastructure(IServiceCollection services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Зарегистрировать продюсеры в DI
    /// </summary>
    Task SetupProducers(IServiceCollection services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Зарегистрировать консьюмеры в DI
    /// </summary>
    Task SetupConsumers(IServiceCollection services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Зарегистрировать RPC-компоненты в DI
    /// </summary>
    Task SetupRpc(IServiceCollection services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Запустить потребление сообщений
    /// </summary>
    Task StartConsumers(IServiceProvider services, BrokerOptionsBuilder builder);

    /// <summary>
    /// Запустить инфраструктуру брокера
    /// </summary>
    Task StartInfrastructure(IServiceProvider serviceProvider, BrokerOptionsBuilder builder);
}
