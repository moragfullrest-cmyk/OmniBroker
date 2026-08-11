# OmniBroker

A unified message broker for .NET with a single API and **RabbitMQ** and **Kafka** providers.

## Solution structure

| Project | Purpose |
|--------|------------|
| `OmniBroker` | Core: interfaces, infrastructure, DI |
| `OmniBroker.RabbitMQ` | RabbitMQ provider |
| `OmniBroker.Kafka` | Kafka provider |
| `OmniBroker.Example` | Usage example |

## Quick start

```bash
dotnet restore OmniBroker.slnx
dotnet build OmniBroker.slnx
```

Wiring in an application:

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
