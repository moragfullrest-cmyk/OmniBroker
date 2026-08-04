using OmniBroker.Infrastructure;

namespace OmniBroker.Kafka.ServiceSetup;

internal class HandlerWrapper
{
    public required Type MessageType { get; init; }
    public required List<Func<IServiceProvider, IMessage, MessageContext, Task<bool>>> Handlers { get; init; }
}
