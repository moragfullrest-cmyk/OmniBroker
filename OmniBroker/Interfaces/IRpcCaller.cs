namespace OmniBroker.Interfaces;

/// <summary>
/// Remote procedure call via the broker
/// </summary>
public interface IRpcCaller<TInputMessage, TOutputMessage>
    where TInputMessage : IMessage
    where TOutputMessage : IMessage
{
    /// <summary>
    /// Send a request and wait for the response
    /// </summary>
    Task<TOutputMessage> Call(TInputMessage input, CancellationToken cancellationToken = default);
}
