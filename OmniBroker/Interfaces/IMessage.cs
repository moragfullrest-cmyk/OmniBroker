namespace OmniBroker;


public interface IMessage
{
    /// <summary>
    /// Routing keys for queue binding. Override on the message type if needed.
    /// </summary>
    string[] GetAcceptableTags() => [""];

    /// <summary>
    /// Message body
    /// </summary>
    public byte[] Body { get; set; }

    /// <summary>
    /// Tag of the specific message
    /// </summary>
    public string Tag { get; set; }

    /// <summary>
    /// Message correlation id
    /// </summary>
    public string CorrelationId { get; set; }
}
