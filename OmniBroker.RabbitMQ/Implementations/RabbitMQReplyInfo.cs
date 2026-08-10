using OmniBroker.Interfaces;

namespace OmniBroker.RabbitMQ.Implementations;

internal sealed class RabbitMQReplyInfo : IReplyInfo
{
    public string ReplyTo { get; set; }
}
