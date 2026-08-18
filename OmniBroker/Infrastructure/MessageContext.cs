using OmniBroker.Interfaces;

namespace OmniBroker.Infrastructure;

internal sealed record MessageContext(
    BrokerId CurrentBrokerId,
    IReplyInfo? ReplyInfo,
    CancellationToken CancellationToken = default);
