namespace OmniBroker.Infrastructure;

/// <summary>
/// Outcome of a consumer handler. There is no third DeadLetter value: transports map
/// <see cref="Retry"/> onto their existing failure path.
/// </summary>
public enum HandleResult
{
    /// <summary>
    /// Handler succeeded. The transport acknowledges the message.
    /// </summary>
    Ack,

    /// <summary>
    /// Transport failure path: RabbitMQ nack with requeue or DLX; Kafka commit plus optional DLQ publish.
    /// </summary>
    Retry
}
