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

    public static async Task EnsureConsumersDeclared(
        IConnection connection,
        INameResolver nameResolver,
        BrokerOptionsBuilder builder,
        string? deadLetterExchange = null)
    {
        ArgumentNullException.ThrowIfNull(connection);

        IChannel channel = await connection.CreateChannelAsync();

        await EnsureDeadLetterTopology(channel, deadLetterExchange);
        IDictionary<string, object?>? queueArgs = DeadLetterQueueArgs(deadLetterExchange);

        foreach (Type type in builder.Consumables.Keys)
        {
            await channel.QueueDeclareAsync(nameResolver.ResolveInboundName(type), false, false, false, queueArgs);
            IMessage prototype = (IMessage)(Activator.CreateInstance(type)
                ?? throw new InvalidOperationException($"Unable to create instance of message type {type}."));
            string[] routingKeys = prototype.GetAcceptableTags() ?? [""];
            foreach (string routingKey in routingKeys)
                await channel.QueueBindAsync(nameResolver.ResolveInboundName(type), nameResolver.ResolveOutboundName(type), routingKey);
        }

        await channel.CloseAsync();
        await channel.DisposeAsync();
    }

    public static async Task EnsureRpcDeclared(
        IConnection connection,
        INameResolver nameResolver,
        BrokerOptionsBuilder builder,
        string? deadLetterExchange = null)
    {
        ArgumentNullException.ThrowIfNull(connection);

        IChannel channel = await connection.CreateChannelAsync();

        await EnsureDeadLetterTopology(channel, deadLetterExchange);
        IDictionary<string, object?>? queueArgs = DeadLetterQueueArgs(deadLetterExchange);

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
            await channel.QueueDeclareAsync(nameResolver.ResolveInboundName(type), false, false, false, queueArgs);
            await channel.QueueBindAsync(nameResolver.ResolveInboundName(type), nameResolver.ResolveOutboundName(type), type.Name);
            await channel.ExchangeDeclareAsync(nameResolver.ResolveOutboundName(returnType), ExchangeType.Topic, true, false);
        }

        await channel.CloseAsync();
        await channel.DisposeAsync();
    }

    private static IDictionary<string, object?>? DeadLetterQueueArgs(string? deadLetterExchange)
        => string.IsNullOrWhiteSpace(deadLetterExchange)
            ? null
            : new Dictionary<string, object?> { ["x-dead-letter-exchange"] = deadLetterExchange };

    private static async Task EnsureDeadLetterTopology(IChannel channel, string? deadLetterExchange)
    {
        if (string.IsNullOrWhiteSpace(deadLetterExchange))
            return;

        await channel.ExchangeDeclareAsync(deadLetterExchange, ExchangeType.Fanout, true, false);
        await channel.QueueDeclareAsync(deadLetterExchange, true, false, false, null);
        await channel.QueueBindAsync(deadLetterExchange, deadLetterExchange, "");
    }
}
