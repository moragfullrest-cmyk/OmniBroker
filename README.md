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

Start via `IHost` / generic host (`builder.Build()` then `Run`). `AddBroker` registers `IHostedService` implementations (`RabbitMqBrokerHostedService` for RabbitMQ, `KafkaConsumer` for Kafka). There is no `UseBrokers` / `UseBrokersAsync`.

Handlers must return `Task<HandleResult>`: `Ack` to confirm, `Retry` to request retry / dead-letter handling.

Retry and dead-letter:

- Kafka `Retry` (and handler exception): the offset is always committed. If `KafkaSettings.DeadLetterTopic` is set, the message is produced to that topic first; a Produce failure still commits the original offset.
- RabbitMQ `Retry` (and handler exception): if `RabbitMQSettings.DeadLetterExchange` is empty, the message is nacked with `requeue: true` (poison-loop risk). If it is set, broker DLX/DLQ topology is used and the message is nacked with `requeue: false`.

RabbitMQ topology notes:

- Consumer-only apps declare the outbound topic exchange before binding, so they do not wait for a producer.
- `DurableQueues` (default true) makes consumable and RPC receive queues durable. Existing non-durable queues with the same name fail with PRECONDITION_FAILED.
- After connection recover, the exclusive RPC reply queue is renamed; pending RPC calls are cancelled.

Wiring in an application:

```csharp
using OmniBroker;
using OmniBroker.RabbitMQ;

builder.Services.AddBroker("orders", options =>
{
    options.UseRabbitMq(new OmniBroker.RabbitMQ.ServiceSetup.RabbitMQSettings
    {
        // ...
    });
    options.AddProducerFor<OrderMessage>();
});

var host = builder.Build();
host.Run();
```

Named brokers use the name as the keyed DI identifier. `SetupName` (queue prefix / Kafka group fallback) defaults to that name and can be overridden **before** `UseRabbitMq` / `UseKafka`.

```csharp
IProducer<OrderMessage> all; // MultiBrokerProducer: publishes to every broker registered for this type
[FromKeyedServices("orders")] IProducer<OrderMessage> orders;
[FromKeyedServices("orders")] IRpcCaller<OrderRequest, OrderReply> rpc;
```

Unnamed `AddBroker(options => ...)` still works; its key is a generated GUID string. Duplicate names throw.
