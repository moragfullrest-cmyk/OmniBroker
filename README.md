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

RabbitMQ topology notes:

- Consumer-only apps declare the outbound topic exchange before binding, so they do not wait for a producer.
- `DurableQueues` (default true) makes consumable and RPC receive queues durable. Existing non-durable queues with the same name fail with PRECONDITION_FAILED.
- After connection recover, the exclusive RPC reply queue is renamed; pending RPC calls are cancelled.

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
