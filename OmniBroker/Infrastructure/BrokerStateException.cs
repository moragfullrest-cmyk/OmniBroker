namespace OmniBroker.Infrastructure;

/// <summary>
/// The remote broker is not in a state this process can use, and the process cannot fix that state itself.
/// </summary>
public sealed class BrokerStateException : Exception
{
    /// <summary>
    /// Creates an exception for a broker that failed a remote state check.
    /// </summary>
    /// <param name="brokerName">Broker name.</param>
    /// <param name="message">Condition that failed. Must not contain credentials.</param>
    public BrokerStateException(string brokerName, string message)
        : this(brokerName, message, null)
    {
    }

    /// <summary>
    /// Creates an exception for a broker that failed a remote state check.
    /// </summary>
    /// <param name="brokerName">Broker name.</param>
    /// <param name="message">Condition that failed. Must not contain credentials.</param>
    /// <param name="innerException">Broker client exception that caused the failure.</param>
    public BrokerStateException(string brokerName, string message, Exception? innerException)
        : base(message, innerException)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(brokerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(message);
        BrokerName = brokerName;
    }

    /// <summary>
    /// Broker name.
    /// </summary>
    public string BrokerName { get; }
}
