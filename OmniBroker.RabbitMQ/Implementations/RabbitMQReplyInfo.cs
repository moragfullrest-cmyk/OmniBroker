using OmniBroker.Interfaces;

namespace OmniBroker.RabbitMQ.Implementations;

internal class RabbitMQReplyInfo : IReplyInfo
{
    public string ReplyTo { get; set; }
}
