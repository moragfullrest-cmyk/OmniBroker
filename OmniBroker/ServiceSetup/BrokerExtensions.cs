using System.Linq.Expressions;
using System.Reflection;
using OmniBroker.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace OmniBroker;

public static class BrokerExtensions
{
    /// <summary>
    /// Запустить работу с брокерами
    /// </summary>
    public static IHost UseBrokers(this IHost host)
        => UseBrokersAsync(host).GetAwaiter().GetResult();

    /// <summary>
    /// Асинхронно запустить работу с брокерами
    /// </summary>
    public static async Task<IHost> UseBrokersAsync(this IHost host)
    {
        foreach (var builder in host.Services.GetServices<BrokerOptionsBuilder>())
        {
            await builder.Extension.StartInfrastructure(host.Services, builder);
            await builder.Extension.StartConsumers(host.Services, builder);
        }

        return host;
    }

    /// <summary>
    /// Входной метод настройки брокера
    /// </summary>
    public static IServiceCollection AddBroker(this IServiceCollection services, Action<BrokerOptionsBuilder> optionsAction)
    {
        ArgumentNullException.ThrowIfNull(optionsAction);

        var options = new BrokerOptionsBuilder();
        optionsAction(options);

        ArgumentNullException.ThrowIfNull(options.Extension);

        options.Extension.BrokerId = options.BrokerId;

        options.Extension.SetupInfrastructure(services, options).GetAwaiter().GetResult();
        options.Extension.SetupProducers(services, options).GetAwaiter().GetResult();
        options.Extension.SetupConsumers(services, options).GetAwaiter().GetResult();
        options.Extension.SetupRpc(services, options).GetAwaiter().GetResult();

        services.AddSingleton(options);
        services.AddKeyedSingleton(options.BrokerId, options);
        return services;
    }

    /// <summary>
    /// Добавить продюсера для сообщения к этому брокеру
    /// </summary>
    public static BrokerOptionsBuilder AddProducerFor<TMessage>(this BrokerOptionsBuilder optionsBuilder) where TMessage : IMessage
    {
        Type inputType = typeof(TMessage);
        EnsureConcreteMessageType(inputType);
        if (optionsBuilder.Producables.Contains(inputType))
            throw new ArgumentException($"Message of type {inputType} is already registered as producer");
        EnsureNotInRpc(optionsBuilder, inputType);
        optionsBuilder.Producables.Add(inputType);
        return optionsBuilder;
    }

    /// <summary>
    /// Добавить потребителя сообщения к этому брокеру. Возможно добавлять несколько потребителей
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

            MethodInfo method = typeof(ServiceProviderServiceExtensions)
                    .GetMethod("GetService", BindingFlags.Static | BindingFlags.Public, [typeof(IServiceProvider)])
                    .MakeGenericMethod(_.ParameterType);

            MethodInfo methodKeyed = typeof(ServiceProviderKeyedServiceExtensions)
                    .GetMethod("GetRequiredKeyedService", BindingFlags.Static | BindingFlags.Public, [typeof(IServiceProvider), typeof(object)])
                    .MakeGenericMethod(_.ParameterType);
            MethodCallExpression simpleCall = Expression.Call(method, provParamExp);
            MethodCallExpression keyedCall = Expression.Call(methodKeyed, provParamExp, Expression.Property(contextParamExp, nameof(MessageContext.CurrentBrokerId)));

            return Expression.Coalesce(simpleCall, keyedCall);
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
