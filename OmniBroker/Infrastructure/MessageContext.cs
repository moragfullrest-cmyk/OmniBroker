using OmniBroker.Interfaces;

namespace OmniBroker.Infrastructure;

internal sealed class MessageContext
{
    public required BrokerId CurrentBrokerId { get; init; }
    public IReplyInfo? ReplyInfo { get; init; }
}
