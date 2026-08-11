using OmniBroker.Interfaces;
using RabbitMQ.Client;

namespace OmniBroker.RabbitMQ.ServiceSetup;

internal sealed class TopologyDeclarer
{
    public static async Task EnsureProducersDeclared(IConnection connection, INameResolver nameResolver, BrokerOptionsBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(connection);

        IChannel channel = await connection.CreateChannelAsync();

        foreach (Type type in builder.Producables)
        {
            await channel.ExchangeDeclareAsync(nameResolver.ResolveOutboundName(type), ExchangeType.Topic, true, false);
        }
        await channel.CloseAsync();
        await channel.DisposeAsync();
    }

    public static async Task EnsureConsumersDeclared(IConnection connection, INameResolver nameResolver, BrokerOptionsBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(connection);

        IChannel channel = await connection.CreateChannelAsync();

        foreach (Type type in builder.Consumables.Keys)
        {
            await channel.QueueDeclareAsync(nameResolver.ResolveInboundName(type), false, false, false, null);
            IMessage prototype = (IMessage)(Activator.CreateInstance(type)
                ?? throw new InvalidOperationException($"Unable to create instance of message type {type}."));
            string[] routingKeys = prototype.GetAcceptableTags() ?? [""];
            foreach (string routingKey in routingKeys)
                await channel.QueueBindAsync(nameResolver.ResolveInboundName(type), nameResolver.ResolveOutboundName(type), routingKey);
        }

        await channel.CloseAsync();
        await channel.DisposeAsync();
    }

    public static async Task EnsureRpcDeclared(IConnection connection, INameResolver nameResolver, BrokerOptionsBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(connection);

        IChannel channel = await connection.CreateChannelAsync();

        var result = await channel.QueueDeclareAsync();
        ((RabbitMQExtension)builder.Extension!).ReplyQueueName = result.QueueName;
        foreach (Type type in builder.RpcCallers.Keys)
        {
            await channel.ExchangeDeclareAsync(nameResolver.ResolveOutboundName(type), ExchangeType.Topic, true, false);
            await channel.ExchangeDeclareAsync(nameResolver.ResolveOutboundName(builder.RpcCallers[type]), ExchangeType.Topic, true, false);
            await channel.QueueBindAsync(result.QueueName, nameResolver.ResolveOutboundName(builder.RpcCallers[type]), result.QueueName);
        }

        foreach (Type type in builder.RpcReceivers.Keys)
        {
            var returnType = builder.RpcReceivers[type].Method.ReturnType.GenericTypeArguments[0];
            await channel.QueueDeclareAsync(nameResolver.ResolveInboundName(type), false, false, false, null);
            await channel.QueueBindAsync(nameResolver.ResolveInboundName(type), nameResolver.ResolveOutboundName(type), type.Name);
            await channel.ExchangeDeclareAsync(nameResolver.ResolveOutboundName(returnType), ExchangeType.Topic, true, false);
        }

        await channel.CloseAsync();
        await channel.DisposeAsync();
    }
}
