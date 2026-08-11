using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OmniBroker.Infrastructure;

namespace OmniBroker;

public static class BrokerExtensions
{
    /// <summary>
    /// Start broker operations
    /// </summary>
    public static IHost UseBrokers(this IHost host)
        => UseBrokersAsync(host).GetAwaiter().GetResult();

    /// <summary>
    /// Start broker operations asynchronously
    /// </summary>
    public static async Task<IHost> UseBrokersAsync(this IHost host)
    {
        foreach (BrokerOptionsBuilder builder in host.Services.GetServices<BrokerOptionsBuilder>())
        {
            await builder.Extension!.StartInfrastructure(host.Services, builder);
            await builder.Extension.StartConsumers(host.Services, builder);
        }

        return host;
    }

    /// <summary>
    /// Entry point for broker configuration
    /// </summary>
    public static IServiceCollection AddBroker(this IServiceCollection services, Action<BrokerOptionsBuilder> optionsAction)
    {
        ArgumentNullException.ThrowIfNull(optionsAction);

        var options = new BrokerOptionsBuilder();
        optionsAction(options);

        ArgumentNullException.ThrowIfNull(options.Extension);

        options.Extension.BrokerId = options.BrokerId;

        if ((options.RpcCallers.Count > 0 || options.RpcReceivers.Count > 0) && !options.Extension.SupportsRpc)
        {
            throw new ArgumentException(
                $"Broker extension '{options.Extension.GetType().Name}' does not support RPC. Remove AddRpcCaller/AddRpcReceiver or use a transport that supports RPC.");
        }

        options.Extension.SetupInfrastructure(services, options).GetAwaiter().GetResult();
        options.Extension.SetupProducers(services, options).GetAwaiter().GetResult();
        options.Extension.SetupConsumers(services, options).GetAwaiter().GetResult();
        options.Extension.SetupRpc(services, options).GetAwaiter().GetResult();

        // Required for startup — keep in the service list
        services.AddSingleton(options);
        services.AddKeyedSingleton(options.BrokerId, options);
        return services;
    }

    /// <summary>
    /// Add a producer for a message type to this broker
    /// </summary>
    public static BrokerOptionsBuilder AddProducerFor<TMessage>(this BrokerOptionsBuilder optionsBuilder) where TMessage : IMessage
    {
        Type inputType = typeof(TMessage);
        EnsureConcreteMessageType(inputType);
        if (optionsBuilder.Producables.Contains(inputType))
            throw new ArgumentException($"Message of type {inputType} is already registered for producing");
        EnsureNotInRpc(optionsBuilder, inputType);
        optionsBuilder.Producables.Add(inputType);
        return optionsBuilder;
    }

    /// <summary>
    /// Add a message consumer to this broker. Multiple consumers may be registered
    /// </summary>
    public static BrokerOptionsBuilder AddConsumerFor<TMessage>(this BrokerOptionsBuilder optionsBuilder, Delegate action)
        where TMessage : IMessage
    {
        ArgumentNullException.ThrowIfNull(action);
        Type messageType = typeof(TMessage);

        EnsureConcreteMessageType(messageType);

        if (action.GetMethodInfo().ReturnType != typeof(Task<bool>))
            throw new ArgumentException("Consuming method has to return Task<bool>");

        if (action.GetMethodInfo().GetParameters().Any(_ => _.ParameterType == typeof(TMessage)) == false)
            throw new ArgumentException($"One of method parameters has to be of type {typeof(TMessage).Name}");

        EnsureNotInRpc(optionsBuilder, messageType);

        if (optionsBuilder.Consumables.ContainsKey(messageType) == false)
            optionsBuilder.Consumables[messageType] = [WrapActionDelegate<bool>(action)];
        else
            optionsBuilder.Consumables[messageType].Add(WrapActionDelegate<bool>(action));

        return optionsBuilder;
    }

    /// <summary>
    /// Register an RPC caller for a message pair
    /// </summary>
    public static BrokerOptionsBuilder AddRpcCaller<TInputMessage, TOutputMessage>(this BrokerOptionsBuilder optionsBuilder)
        where TInputMessage : IMessage
        where TOutputMessage : IMessage
    {
        Type inputType = typeof(TInputMessage);
        Type outputType = typeof(TOutputMessage);

        EnsureConcreteMessageType(inputType);
        EnsureConcreteMessageType(outputType);
        EnsureNotInRegularMessaging(optionsBuilder, inputType);
        EnsureNotInRegularMessaging(optionsBuilder, outputType);

        if (optionsBuilder.RpcCallers.ContainsKey(inputType))
            throw new ArgumentException($"RPC caller for input type {inputType} is already registered");
        if (optionsBuilder.RpcCallers.ContainsValue(outputType))
            throw new ArgumentException($"Message of type {outputType} is already registered as RPC output");
        if (optionsBuilder.RpcCallers.ContainsKey(outputType))
            throw new ArgumentException($"Message of type {outputType} is already registered as RPC input");
        if (optionsBuilder.RpcCallers.ContainsValue(inputType))
            throw new ArgumentException($"Message of type {inputType} is already registered as RPC output");
        if (optionsBuilder.RpcReceivers.ContainsKey(outputType))
            throw new ArgumentException($"Message of type {outputType} is already registered as RPC receive input");
        // RpcReceivers.ContainsKey(inputType) is allowed: receiver may already be registered for this pair.

        optionsBuilder.RpcCallers.Add(inputType, outputType);
        return optionsBuilder;
    }

    /// <summary>
    /// Register an RPC receiver with a request handler
    /// </summary>
    public static BrokerOptionsBuilder AddRpcReceiver<TInputMessage, TOutputMessage>(this BrokerOptionsBuilder optionsBuilder, Delegate action)
        where TInputMessage : IMessage
        where TOutputMessage : IMessage
    {
        ArgumentNullException.ThrowIfNull(action);

        Type inputType = typeof(TInputMessage);
        Type outputType = typeof(TOutputMessage);

        EnsureConcreteMessageType(inputType);
        EnsureConcreteMessageType(outputType);

        if (action.GetMethodInfo().ReturnType != typeof(Task<TOutputMessage>))
            throw new ArgumentException($"Consuming method has to return Task<{nameof(TOutputMessage)}>");

        if (action.GetMethodInfo().GetParameters().Any(_ => _.ParameterType == typeof(TInputMessage)) == false)
            throw new ArgumentException($"One of method parameters has to be of type {nameof(TInputMessage)}");

        EnsureNotInRegularMessaging(optionsBuilder, inputType);
        EnsureNotInRegularMessaging(optionsBuilder, outputType);

        if (optionsBuilder.RpcReceivers.ContainsKey(inputType))
            throw new ArgumentException($"RPC receiver for input type {inputType} is already registered");

        if (optionsBuilder.RpcCallers.ContainsKey(inputType))
        {
            if (optionsBuilder.RpcCallers[inputType] != outputType)
                throw new ArgumentException($"RPC receiver output type {outputType} does not match registered caller output {optionsBuilder.RpcCallers[inputType]}");
            // Matching caller already registered for this pair — allow.
        }
        else
        {
            if (optionsBuilder.RpcCallers.ContainsValue(inputType))
                throw new ArgumentException($"Message of type {inputType} is already registered as RPC output");
            if (optionsBuilder.RpcCallers.ContainsKey(outputType))
                throw new ArgumentException($"Message of type {outputType} is already registered as RPC input");
            if (optionsBuilder.RpcCallers.ContainsValue(outputType))
                throw new ArgumentException($"Message of type {outputType} is already registered as RPC output");
        }

        if (optionsBuilder.RpcReceivers.ContainsKey(outputType))
            throw new ArgumentException($"Message of type {outputType} is already registered as RPC receive input");

        optionsBuilder.RpcReceivers.Add(inputType, action);
        return optionsBuilder;
    }

    private static void EnsureConcreteMessageType(Type messageType)
    {
        if (messageType.IsAbstract || messageType.IsInterface)
            throw new ArgumentException("Message type has to be a concrete non abstract type");
    }

    private static void EnsureNotInRegularMessaging(BrokerOptionsBuilder optionsBuilder, Type messageType)
    {
        if (optionsBuilder.Producables.Contains(messageType))
            throw new ArgumentException($"Message of type {messageType} is already registered as producer");
        if (optionsBuilder.Consumables.ContainsKey(messageType))
            throw new ArgumentException($"Message of type {messageType} is already registered as consumer");
    }

    private static void EnsureNotInRpc(BrokerOptionsBuilder optionsBuilder, Type messageType)
    {
        if (optionsBuilder.RpcCallers.ContainsKey(messageType)
            || optionsBuilder.RpcCallers.ContainsValue(messageType)
            || optionsBuilder.RpcReceivers.ContainsKey(messageType))
            throw new ArgumentException($"Message of type {messageType} is already registered for RPC messaging");
    }

    internal static Func<IServiceProvider, IMessage, MessageContext, Task<TResult>> WrapActionDelegate<TResult>(Delegate @delegate)
    {

        ParameterExpression provParamExp = Expression.Parameter(typeof(IServiceProvider), "provider");
        ParameterExpression messParamExp = Expression.Parameter(typeof(IMessage), "message");
        ParameterExpression contextParamExp = Expression.Parameter(typeof(MessageContext), "context");

        IEnumerable<Expression> parameters = @delegate.GetMethodInfo().GetParameters().Select<ParameterInfo, Expression>(_ =>
        {
            if (_.ParameterType == typeof(IServiceProvider))
                return provParamExp;
            if (_.ParameterType.IsAssignableTo(typeof(IMessage)))
                return Expression.Convert(messParamExp, _.ParameterType);
            if (_.ParameterType == typeof(MessageContext))
            {
                return contextParamExp;
            }

            // 1) soft keyed by CurrentBrokerId (broker infrastructure)
            // 2) required keyed by [FromKeyedServices] if present
            // 3) otherwise required unkeyed
            MethodInfo getKeyedService = typeof(ServiceProviderKeyedServiceExtensions)
                    .GetMethod(nameof(ServiceProviderKeyedServiceExtensions.GetKeyedService), BindingFlags.Static | BindingFlags.Public, [typeof(IServiceProvider), typeof(object)])!
                    .MakeGenericMethod(_.ParameterType);

            Expression softBrokerCall = Expression.Call(
                getKeyedService,
                provParamExp,
                Expression.Property(contextParamExp, nameof(MessageContext.CurrentBrokerId)));

            FromKeyedServicesAttribute? keyedAttr = _.GetCustomAttribute<FromKeyedServicesAttribute>();
            Expression fallbackCall;
            if (keyedAttr is not null)
            {
                MethodInfo getRequiredKeyed = typeof(ServiceProviderKeyedServiceExtensions)
                        .GetMethod(nameof(ServiceProviderKeyedServiceExtensions.GetRequiredKeyedService), BindingFlags.Static | BindingFlags.Public, [typeof(IServiceProvider), typeof(object)])!
                        .MakeGenericMethod(_.ParameterType);
                fallbackCall = Expression.Call(
                    getRequiredKeyed,
                    provParamExp,
                    Expression.Constant(keyedAttr.Key, typeof(object)));
            }
            else
            {
                MethodInfo getRequired = typeof(ServiceProviderServiceExtensions)
                        .GetMethod(nameof(ServiceProviderServiceExtensions.GetRequiredService), BindingFlags.Static | BindingFlags.Public, [typeof(IServiceProvider)])!
                        .MakeGenericMethod(_.ParameterType);
                fallbackCall = Expression.Call(getRequired, provParamExp);
            }

            return Expression.Coalesce(softBrokerCall, fallbackCall);
        });

        Expression callExp;

        if (@delegate.Method.IsStatic)
        {
            callExp = Expression.Call(@delegate.Method, parameters);
        }
        else
        {
            callExp = Expression.Call(Expression.Constant(@delegate.Target), @delegate.Method, parameters);
        }

        var lambda = Expression.Lambda<Func<IServiceProvider, IMessage, MessageContext, Task<TResult>>>(callExp, provParamExp, messParamExp, contextParamExp);

        return lambda.Compile();
    }
}
