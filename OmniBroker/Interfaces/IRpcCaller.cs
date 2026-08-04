namespace OmniBroker.Interfaces;

public interface IRpcCaller<TInputMessage, TOutputMessage>
    where TInputMessage : IMessage
    where TOutputMessage : IMessage
{
    Task<TOutputMessage> Call(TInputMessage input);
}
