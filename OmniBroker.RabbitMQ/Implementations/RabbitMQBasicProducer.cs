using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.ServiceSetup;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace OmniBroker.RabbitMQ.Implementations;

internal sealed class RabbitMQBasicProducer<TMessage>(
    ILogger<RabbitMQBasicProducer<TMessage>> logger,
    ConcurrentObjectPool<IChannel> channelPool,
    INameResolver nameResolver
    )
    : IProducer<TMessage> where TMessage : IMessage
{
    public async Task<bool> Publish(TMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
    {
        IChannel channel = await channelPool.GetAsync(cancellationToken).ConfigureAwait(false);
        var returned = false;
        try
        {
            if (channel.IsClosed)
            {
                await channelPool.DiscardAsync(channel).ConfigureAwait(false);
                returned = true;
                return false;
            }

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
                cancellationToken: cancellationToken).ConfigureAwait(false);

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
            if (!returned)
            {
                if (channel.IsOpen)
                    channelPool.Return(channel);
                else
                    await channelPool.DiscardAsync(channel).ConfigureAwait(false);
            }
        }
    }
}
