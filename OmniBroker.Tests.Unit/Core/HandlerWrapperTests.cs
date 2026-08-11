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
        var brokerId = new BrokerId();
        var services = new ServiceCollection();
        services.AddKeyedSingleton(brokerId, new TestDependency { Value = "broker-keyed" });
        services.AddKeyedSingleton("explicit-key", new TestDependency { Value = "explicit" });
        ServiceProvider provider = services.BuildServiceProvider();

        Func<IServiceProvider, IMessage, MessageContext, Task<bool>> softKeyed =
            BrokerExtensions.WrapActionDelegate<bool>(
                (TestMessage message, TestDependency dep) =>
                {
                    dep.Value.ShouldBe("broker-keyed");
                    message.ShouldNotBeNull();
                    return Task.FromResult(true);
                });

        // FromKeyedServices is only used when soft lookup by CurrentBrokerId returns null.
        var otherBrokerId = new BrokerId();
        Func<IServiceProvider, IMessage, MessageContext, Task<bool>> fromKeyed =
            BrokerExtensions.WrapActionDelegate<bool>(
                ([FromKeyedServices("explicit-key")] TestDependency dep, TestMessage message) =>
                {
                    dep.Value.ShouldBe("explicit");
                    return Task.FromResult(true);
                });

        (await softKeyed(provider, new TestMessage(), new MessageContext(brokerId, null))).ShouldBeTrue();
        (await fromKeyed(provider, new TestMessage(), new MessageContext(otherBrokerId, null))).ShouldBeTrue();
    }

    [Fact]
    public async Task WrapActionDelegate_falls_back_to_unkeyed_when_broker_key_missing()
    {
        var brokerId = new BrokerId();
        var services = new ServiceCollection();
        services.AddSingleton(new TestDependency { Value = "unkeyed" });
        ServiceProvider provider = services.BuildServiceProvider();

        Func<IServiceProvider, IMessage, MessageContext, Task<bool>> wrapped =
            BrokerExtensions.WrapActionDelegate<bool>(
                (TestMessage _, TestDependency dep) =>
                {
                    dep.Value.ShouldBe("unkeyed");
                    return Task.FromResult(true);
                });

        bool result = await wrapped(provider, new TestMessage(), new MessageContext(brokerId, null));

        result.ShouldBeTrue();
    }
}
