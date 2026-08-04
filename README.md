# OmniBroker

Обобщённый брокер сообщений для .NET с единым API и провайдерами **RabbitMQ** и **Kafka**.

## Структура решения

| Проект | Назначение |
|--------|------------|
| `OmniBroker` | Ядро: интерфейсы, инфраструктура, DI |
| `OmniBroker.RabbitMQ` | Провайдер RabbitMQ |
| `OmniBroker.Kafka` | Провайдер Kafka |
| `OmniBroker.Example` | Пример использования |

## Быстрый старт

```bash
dotnet restore OmniBroker.slnx
dotnet build OmniBroker.slnx
```

Подключение в приложении:

```csharp
using OmniBroker;
using OmniBroker.RabbitMQ;

builder.Services.AddBroker(options =>
{
    options.UseRabbitMq(new OmniBroker.RabbitMQ.ServiceSetup.RabbitMQSettings
    {
        // ...
    });
});
```
