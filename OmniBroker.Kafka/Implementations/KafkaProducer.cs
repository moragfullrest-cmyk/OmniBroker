
using Confluent.Kafka;
using OmniBroker.Interfaces;
using Microsoft.Extensions.Logging;

namespace OmniBroker.Kafka.Implementations;

internal class KafkaProducer<TMessage>(
    ILogger<KafkaProducer<TMessage>> logger,
    IProducer<string, byte[]> producer,
    INameResolver nameResolver
    ) : IProducer<TMessage> where TMessage : IMessage
{
    public async Task<bool> Publish(TMessage message)
    {
        try
        {
            Type messageType = typeof(TMessage);
            string topicName = nameResolver.ResolveOutboundName(messageType);
            await producer.ProduceAsync(topicName, new Message<string, byte[]> { Key = message.Tag, Value = message.Body });
            return true;
        }
        catch (ProduceException<Null, string> e)
        {
            logger.LogWarning(e, "Delivery failed: {Reason}", e.Error.Reason);
            return false;

        }
    }
}
