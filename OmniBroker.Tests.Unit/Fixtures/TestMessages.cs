namespace OmniBroker.Tests.Unit.Fixtures;

public sealed class TestMessage : IMessage
{
    public byte[] Body { get; set; } = [];
    public string Tag { get; set; } = "";
    public string CorrelationId { get; set; } = "";
}

public sealed class TestReplyMessage : IMessage
{
    public byte[] Body { get; set; } = [];
    public string Tag { get; set; } = "";
    public string CorrelationId { get; set; } = "";
}

public sealed class AnotherTestMessage : IMessage
{
    public byte[] Body { get; set; } = [];
    public string Tag { get; set; } = "";
    public string CorrelationId { get; set; } = "";
}

public abstract class AbstractTestMessage : IMessage
{
    public byte[] Body { get; set; } = [];
    public string Tag { get; set; } = "";
    public string CorrelationId { get; set; } = "";
}

public sealed class MessageWithoutParameterlessCtor : IMessage
{
    public MessageWithoutParameterlessCtor(string value)
    {
        Tag = value;
    }

    public byte[] Body { get; set; } = [];
    public string Tag { get; set; }
    public string CorrelationId { get; set; } = "";
}

public sealed class TestDependency
{
    public string Value { get; init; } = "unkeyed";
}
