using System.Globalization;
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

            kafkaMessage.Headers = CreateHeaders(options, message.CorrelationId);

            DeliveryResult<string, byte[]> deliveryResult = ReplyProducePartition(options) is int partition
                ? await producer.ProduceAsync(new TopicPartition(topicName, new Partition(partition)), kafkaMessage, cancellationToken)
                : await producer.ProduceAsync(topicName, kafkaMessage, cancellationToken);

            if (deliveryResult.Status == PersistenceStatus.NotPersisted)
            {
                logger.LogWarning(
                    "Delivery failed: message was not persisted to {Topic}. Status: {Status}",
                    deliveryResult.Topic,
                    deliveryResult.Status);
                return false;
            }

            return true;
        }
        catch (ProduceException<string, byte[]> e)
        {
            logger.LogWarning(e, "Delivery failed: {Reason}", e.Error.Reason);
            return false;
        }
    }

    private static Headers? CreateHeaders(PublishOptions? options, string? messageCorrelationId)
    {
        string? correlationId = options?.CorrelationId ?? messageCorrelationId;
        string? replyTopic = options?.ReplyTo;
        Headers? headers = null;

        if (!string.IsNullOrEmpty(correlationId))
            Add(KafkaMessageHeaders.CorrelationId, correlationId);
        if (!string.IsNullOrWhiteSpace(replyTopic))
            Add(KafkaMessageHeaders.ReplyTopic, replyTopic);
        if (options is KafkaPublishOptions { ReplyPartition: int replyPartition } && !string.IsNullOrWhiteSpace(replyTopic))
            Add(KafkaMessageHeaders.ReplyPartition, replyPartition.ToString(CultureInfo.InvariantCulture));

        return headers;

        void Add(string key, string value)
        {
            headers ??= new Headers();
            headers.Add(key, Encoding.UTF8.GetBytes(value));
        }
    }

    private static int? ReplyProducePartition(PublishOptions? options)
    {
        if (options is not KafkaPublishOptions { ReplyPartition: int replyPartition })
            return null;

        return string.IsNullOrWhiteSpace(options.ReplyTo) ? replyPartition : null;
    }
}
