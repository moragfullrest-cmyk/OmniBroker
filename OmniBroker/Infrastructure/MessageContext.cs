using OmniBroker.Interfaces;

namespace OmniBroker.Infrastructure;

internal sealed record MessageContext(
    string CurrentBrokerName,
    IReplyInfo? ReplyInfo,
    CancellationToken CancellationToken = default);
