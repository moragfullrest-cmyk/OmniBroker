using Microsoft.Extensions.Logging;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ.ServiceSetup;
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
        ArgumentNullException.ThrowIfNull(message);

        IChannel channel = await channelPool.GetAsync(cancellationToken).ConfigureAwait(false);
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
                cancellationToken: cancellationToken).ConfigureAwait(false);

            return true;
        }
        catch (PublishException publishException)
        {
            logger.LogError(publishException, "Message publish failed");

            return false;
        }
        catch (RabbitMQClientException rabbitMQClientException)
        {
            logger.LogError(rabbitMQClientException, "RabbitMQ client error");

            return false;
        }
        finally
        {
            if (channel.IsOpen)
                channelPool.Return(channel);
            else
                await channelPool.DiscardAsync(channel).ConfigureAwait(false);
        }
    }
}
