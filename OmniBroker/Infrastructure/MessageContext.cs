using OmniBroker.Interfaces;

namespace OmniBroker.Infrastructure;

internal sealed class MessageContext
{
    public BrokerId CurrentBrokerId { get; set; }
    public IReplyInfo ReplyInfo { get; set; }
}
