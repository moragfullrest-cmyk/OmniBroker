using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;

namespace OmniBroker.Kafka.Implementations;

internal sealed class KafkaProducer<TMessage>(
    ILogger<KafkaProducer<TMessage>> logger,
    IProducer<string, byte[]> producer,
    INameResolver nameResolver
    ) : OmniBroker.IProducer<TMessage> where TMessage : IMessage
{
    public async Task<bool> Publish(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);

        try
        {
            string topicName = options?.Destination ?? nameResolver.ResolveOutboundName(typeof(TMessage));
            var kafkaMessage = new Message<string, byte[]>
            {
                Key = message.Tag,
                Value = message.Body
            };

            string? correlationId = options?.CorrelationId ?? message.CorrelationId;
            if (!string.IsNullOrEmpty(correlationId))
            {
                kafkaMessage.Headers = new Headers
                {
                    { KafkaMessageHeaders.CorrelationId, Encoding.UTF8.GetBytes(correlationId) }
                };
            }

            await producer.ProduceAsync(topicName, kafkaMessage, cancellationToken);
            return true;
        }
        catch (ProduceException<string, byte[]> e)
        {
            logger.LogWarning(e, "Delivery failed: {Reason}", e.Error.Reason);
            return false;
        }
    }
}
