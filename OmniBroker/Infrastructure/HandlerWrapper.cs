using System.Linq.Expressions;
using System.Reflection;

namespace OmniBroker.Infrastructure;

internal sealed record HandlerWrapper(
    Type MessageType,
    List<Func<IServiceProvider, IMessage, MessageContext, Task<bool>>> Handlers,
    Func<IMessage> CreateMessage)
{
    public static Func<IMessage> BuildCreateMessage(Type messageType)
    {
        ConstructorInfo? ctor = messageType.GetConstructor([]);
        if (ctor == null)
        {
            throw new MissingMethodException(messageType.FullName, "Message must have a parameterless constructor");
        }

        NewExpression newExp = Expression.New(ctor);
        var expr = Expression.Lambda<Func<IMessage>>(newExp);
        return expr.Compile();
    }
}
