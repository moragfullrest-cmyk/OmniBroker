using OmniBroker.RabbitMQ.ServiceSetup;
using Shouldly;

namespace OmniBroker.Tests.Unit.RabbitMq;

public sealed class ConcurrentObjectPoolTests
{
    private sealed class PoolItem : IDisposable
    {
        public bool Disposed { get; private set; }
        public void Dispose() => Disposed = true;
    }

    [Fact]
    public async Task Get_creates_and_Return_reuses()
    {
        int created = 0;
        await using var pool = new ConcurrentObjectPool<PoolItem>(
            _ =>
            {
                Interlocked.Increment(ref created);
                return Task.FromResult(new PoolItem());
            },
            maxSize: 2);

        PoolItem first = await pool.GetAsync();
        pool.Return(first);
        PoolItem second = await pool.GetAsync();

        second.ShouldBeSameAs(first);
        created.ShouldBe(1);
    }

    [Fact]
    public async Task Get_waits_when_pool_full_until_Return()
    {
        await using var pool = new ConcurrentObjectPool<PoolItem>(
            _ => Task.FromResult(new PoolItem()),
            maxSize: 1);

        PoolItem held = await pool.GetAsync();
        Task<PoolItem> waiting = pool.GetAsync();
        await Task.Delay(50);
        waiting.IsCompleted.ShouldBeFalse();

        pool.Return(held);
        PoolItem released = await waiting.WaitAsync(TimeSpan.FromSeconds(2));

        released.ShouldBeSameAs(held);
    }

    [Fact]
    public async Task DiscardAsync_frees_slot_for_new_create()
    {
        int created = 0;
        await using var pool = new ConcurrentObjectPool<PoolItem>(
            _ =>
            {
                Interlocked.Increment(ref created);
                return Task.FromResult(new PoolItem());
            },
            maxSize: 1);

        PoolItem first = await pool.GetAsync();
        await pool.DiscardAsync(first);
        PoolItem second = await pool.GetAsync();

        second.ShouldNotBeSameAs(first);
        first.Disposed.ShouldBeTrue();
        created.ShouldBe(2);
    }

    [Fact]
    public async Task Get_after_DisposeAsync_throws()
    {
        var pool = new ConcurrentObjectPool<PoolItem>(_ => Task.FromResult(new PoolItem()), maxSize: 1);
        await pool.DisposeAsync();

        await Should.ThrowAsync<ObjectDisposedException>(() => pool.GetAsync());
    }

    [Fact]
    public async Task Return_after_dispose_disposes_item()
    {
        var pool = new ConcurrentObjectPool<PoolItem>(_ => Task.FromResult(new PoolItem()), maxSize: 1);
        PoolItem item = await pool.GetAsync();
        await pool.DisposeAsync();

        pool.Return(item);

        item.Disposed.ShouldBeTrue();
    }

    [Fact]
    public async Task Generator_failure_decrements_created_and_allows_retry()
    {
        int attempts = 0;
        await using var pool = new ConcurrentObjectPool<PoolItem>(
            _ =>
            {
                if (Interlocked.Increment(ref attempts) == 1)
                    throw new InvalidOperationException("boom");
                return Task.FromResult(new PoolItem());
            },
            maxSize: 1);

        await Should.ThrowAsync<InvalidOperationException>(() => pool.GetAsync());
        PoolItem item = await pool.GetAsync();

        item.ShouldNotBeNull();
        attempts.ShouldBe(2);
    }

    [Fact]
    public async Task Return_null_throws()
    {
        await using var pool = new ConcurrentObjectPool<PoolItem>(_ => Task.FromResult(new PoolItem()), maxSize: 1);

        Should.Throw<ArgumentNullException>(() => pool.Return(null!));
    }
}
