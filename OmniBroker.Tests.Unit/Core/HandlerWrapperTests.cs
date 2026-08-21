using Microsoft.Extensions.DependencyInjection;
using OmniBroker.Infrastructure;
using OmniBroker.Tests.Unit.Fixtures;
using Shouldly;

namespace OmniBroker.Tests.Unit.Core;

public sealed class HandlerWrapperTests
{
    [Fact]
    public void BuildCreateMessage_with_parameterless_ctor_creates_instance()
    {
        Func<IMessage> factory = HandlerWrapper.BuildCreateMessage(typeof(TestMessage));

        IMessage message = factory();

        message.ShouldBeOfType<TestMessage>();
    }

    [Fact]
    public void BuildCreateMessage_without_parameterless_ctor_throws()
    {
        Should.Throw<MissingMethodException>(() =>
            HandlerWrapper.BuildCreateMessage(typeof(MessageWithoutParameterlessCtor)));
    }

    [Fact]
    public async Task WrapActionDelegate_resolves_unkeyed_and_keyed_dependencies()
    {
        var brokerId = Guid.NewGuid().ToString();
        var services = new ServiceCollection();
        services.AddKeyedSingleton(brokerId, new TestDependency { Value = "broker-keyed" });
        services.AddKeyedSingleton("explicit-key", new TestDependency { Value = "explicit" });
        ServiceProvider provider = services.BuildServiceProvider();

        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> softKeyed =
            BrokerExtensions.WrapActionDelegate<HandleResult>(
                (TestMessage message, TestDependency dep) =>
                {
                    dep.Value.ShouldBe("broker-keyed");
                    message.ShouldNotBeNull();
                    return Task.FromResult(HandleResult.Ack);
                });

        // FromKeyedServices is only used when soft lookup by CurrentBrokerId returns null.
        var otherBrokerId = Guid.NewGuid().ToString();
        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> fromKeyed =
            BrokerExtensions.WrapActionDelegate<HandleResult>(
                ([FromKeyedServices("explicit-key")] TestDependency dep, TestMessage message) =>
                {
                    dep.Value.ShouldBe("explicit");
                    return Task.FromResult(HandleResult.Ack);
                });

        (await softKeyed(provider, new TestMessage(), new MessageContext(brokerId, null))).ShouldBe(HandleResult.Ack);
        (await fromKeyed(provider, new TestMessage(), new MessageContext(otherBrokerId, null))).ShouldBe(HandleResult.Ack);
    }

    [Fact]
    public async Task WrapActionDelegate_falls_back_to_unkeyed_when_broker_key_missing()
    {
        var brokerId = Guid.NewGuid().ToString();
        var services = new ServiceCollection();
        services.AddSingleton(new TestDependency { Value = "unkeyed" });
        ServiceProvider provider = services.BuildServiceProvider();

        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> wrapped =
            BrokerExtensions.WrapActionDelegate<HandleResult>(
                (TestMessage _, TestDependency dep) =>
                {
                    dep.Value.ShouldBe("unkeyed");
                    return Task.FromResult(HandleResult.Ack);
                });

        HandleResult result = await wrapped(provider, new TestMessage(), new MessageContext(brokerId, null));

        result.ShouldBe(HandleResult.Ack);
    }

    [Fact]
    public async Task WrapActionDelegate_passes_CancellationToken_from_MessageContext()
    {
        using var cts = new CancellationTokenSource();
        Func<IServiceProvider, IMessage, MessageContext, Task<HandleResult>> wrapped =
            BrokerExtensions.WrapActionDelegate<HandleResult>(
                (TestMessage _, CancellationToken token) =>
                {
                    token.ShouldBe(cts.Token);
                    return Task.FromResult(HandleResult.Ack);
                });

        HandleResult result = await wrapped(
            new ServiceCollection().BuildServiceProvider(),
            new TestMessage(),
            new MessageContext(Guid.NewGuid().ToString(), null, cts.Token));

        result.ShouldBe(HandleResult.Ack);
    }
}
