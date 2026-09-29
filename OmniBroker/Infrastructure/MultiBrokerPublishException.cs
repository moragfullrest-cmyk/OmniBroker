namespace OmniBroker.Infrastructure;

/// <summary>
/// Outcome of one broker inside a multi-broker publish.
/// </summary>
/// <param name="BrokerId">Keyed broker identifier.</param>
/// <param name="ProducerType">Runtime type name of the producer that was invoked.</param>
/// <param name="Succeeded">Whether that producer accepted the message.</param>
public sealed record BrokerPublishOutcome(string BrokerId, string ProducerType, bool Succeeded);

/// <summary>
/// Thrown when a multi-broker publish is not atomic: at least one producer failed after one or more others may already have published.
/// </summary>
public sealed class MultiBrokerPublishException : Exception
{
    public MultiBrokerPublishException(Type messageType, IReadOnlyList<BrokerPublishOutcome> outcomes)
        : base(BuildMessage(messageType, outcomes))
    {
        ArgumentNullException.ThrowIfNull(messageType);
        ArgumentNullException.ThrowIfNull(outcomes);
        MessageType = messageType;
        Outcomes = outcomes;
    }

    /// <summary>
    /// Message type that was published.
    /// </summary>
    public Type MessageType { get; }

    /// <summary>
    /// One entry per invoked producer, in publish order.
    /// </summary>
    public IReadOnlyList<BrokerPublishOutcome> Outcomes { get; }

    private static string BuildMessage(Type messageType, IReadOnlyList<BrokerPublishOutcome> outcomes)
    {
        string failed = string.Join(", ", outcomes.Where(outcome => !outcome.Succeeded).Select(outcome => outcome.BrokerId));
        string succeeded = string.Join(", ", outcomes.Where(outcome => outcome.Succeeded).Select(outcome => outcome.BrokerId));
        if (string.IsNullOrEmpty(succeeded))
            return $"Publish of {messageType.Name} failed for broker(s): {failed}.";

        return $"Publish of {messageType.Name} failed for broker(s): {failed}. Already published to: {succeeded}.";
    }
}
