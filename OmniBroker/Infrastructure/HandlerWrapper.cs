using OmniBroker.Interfaces;

namespace OmniBroker.Infrastructure;

internal sealed class HandlerWrapper
{
    public required Type MessageType { get; init; }
    public required List<Func<IServiceProvider, IMessage, MessageContext, Task<bool>>> Handlers { get; init; }
}
