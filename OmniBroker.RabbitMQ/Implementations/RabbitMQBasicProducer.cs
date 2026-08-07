using OmniBroker.Infrastructure;
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
    public async Task<bool> Publish(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        IChannel channel = channelPool.Get();
        try
        {
            if (channel.IsClosed)
                return false;

            string exchange = options?.Destination ?? nameResolver.ResolveOutboundName(typeof(TMessage));

            var props = new BasicProperties
            {
                CorrelationId = options?.CorrelationId ?? Guid.NewGuid().ToString(),
                ReplyTo = options?.ReplyTo
            };

            await channel.BasicPublishAsync(
                exchange: exchange,
                routingKey: string.IsNullOrEmpty(message.Tag) ? "" : message.Tag,
                mandatory: true,
                body: message.Body,
                basicProperties: props,
                cancellationToken: cancellationToken);

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
