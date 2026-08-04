using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.ServiceSetup;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace OmniBroker.RabbitMQ.Implementations;

internal class RabbitMQBasicProducer<TMessage>(
    ILogger<RabbitMQBasicProducer<TMessage>> logger,
    ConcurrentObjectPool<IChannel> channelPool,
    INameResolver nameResolver
    )
    : IProducer<TMessage> where TMessage : IMessage
{
    internal string ReplyQueue { get; set; }
    internal string ExchangeName = nameResolver.ResolveOutboundName(typeof(TMessage));
    internal string? CorrelationId;
    public async Task<bool> Publish(TMessage message)
    {
        IChannel channel = channelPool.Get();
        try
        {
            if (channel.IsClosed)
                return false;

            var props = new BasicProperties
            {
                CorrelationId = CorrelationId ?? Guid.NewGuid().ToString(),
                ReplyTo = ReplyQueue
            };

            await channel.BasicPublishAsync(exchange: ExchangeName, routingKey: string.IsNullOrEmpty(message.Tag) ? "" : message.Tag, mandatory: true, body: message.Body, basicProperties: props);

            return true;
        }
        catch (PublishException publishException)
        {
            logger.LogError(publishException, "Ошибка публикации сообщения");

            return false;
        }
        catch (RabbitMQClientException rabbitMQClientException)
        {
            logger.LogError(rabbitMQClientException, "Ошибка клиента RabbitMQ");

            return false;
        }
        finally
        {
            if (channel.IsOpen)
                channelPool.Return(channel);
        }
    }
}
