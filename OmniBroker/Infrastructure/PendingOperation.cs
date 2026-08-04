namespace OmniBroker.Infrastructure;

internal record PendingOperation
{
    public bool Timedout { get; set; }
    public DateTime CallTime { get; set; }
    public IMessage? Result { get; set; }
}
