namespace OmniBroker.Tests.Unit.Fixtures.Left
{
    public sealed class WireNameMessage : IMessage
    {
        public byte[] Body { get; set; } = [];
        public string Tag { get; set; } = "";
        public string CorrelationId { get; set; } = "";
    }
}

namespace OmniBroker.Tests.Unit.Fixtures.Right
{
    public sealed class WireNameMessage : IMessage
    {
        public byte[] Body { get; set; } = [];
        public string Tag { get; set; } = "";
        public string CorrelationId { get; set; } = "";
    }
}
