using OmniBroker.Interfaces;

namespace OmniBroker.Infrastructure;

internal sealed record MessageContext(
    string CurrentBrokerId,
    IReplyInfo? ReplyInfo,
    CancellationToken CancellationToken = default);
