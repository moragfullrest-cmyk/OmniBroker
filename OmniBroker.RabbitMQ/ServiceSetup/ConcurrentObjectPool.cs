using System.Collections.Concurrent;

namespace OmniBroker.RabbitMQ.ServiceSetup;

public sealed class ConcurrentObjectPool<T> : IAsyncDisposable
    where T : class
{
    private readonly ConcurrentBag<T> _objects = [];
    private readonly Func<CancellationToken, Task<T>> _objectGenerator;
    private readonly SemaphoreSlim _slots;
    private readonly object _gate = new();
    private int _disposed;

    public ConcurrentObjectPool(Func<CancellationToken, Task<T>> objectGenerator, int maxSize = 32)
    {
        _objectGenerator = objectGenerator ?? throw new ArgumentNullException(nameof(objectGenerator));
        if (maxSize < 1) throw new ArgumentOutOfRangeException(nameof(maxSize));
        _slots = new SemaphoreSlim(maxSize, maxSize);
    }

    public async Task<T> GetAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
                if (_objects.TryTake(out T? item))
                    return item;
            }

            T created = (await _objectGenerator(cancellationToken).ConfigureAwait(false))!;
            lock (_gate)
            {
                if (Volatile.Read(ref _disposed) == 0)
                    return created;
            }

            await DisposeItemAsync(created).ConfigureAwait(false);
            throw new ObjectDisposedException(GetType().FullName);
        }
        catch
        {
            ReleaseSlot();
            throw;
        }
    }

    public void Return(T item)
    {
        ArgumentNullException.ThrowIfNull(item);
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                DisposeItem(item);
                return;
            }

            _objects.Add(item);
        }

        ReleaseSlot();
    }

    /// <summary>
    /// Drop a broken item and free the slot it held so a new one can be created.
    /// </summary>
    public async ValueTask DiscardAsync(T item)
    {
        ArgumentNullException.ThrowIfNull(item);
        await DisposeItemAsync(item).ConfigureAwait(false);
        ReleaseSlot();
    }

    /// <summary>
    /// Dispose idle items currently in the bag without disposing the pool.
    /// In-flight items are left to the caller (<see cref="Return"/> or <see cref="DiscardAsync"/>).
    /// Those idle items already released their slots on <see cref="Return"/>, so this method does not release slots.
    /// </summary>
    public async ValueTask ClearAsync()
    {
        while (true)
        {
            T? item;
            lock (_gate)
            {
                if (!_objects.TryTake(out item))
                    return;
            }

            await DisposeItemAsync(item).ConfigureAwait(false);
        }
    }

    public async ValueTask DisposeAsync()
    {
        List<T> idle;
        lock (_gate)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            idle = [];
            while (_objects.TryTake(out T? item))
                idle.Add(item);
        }

        foreach (T item in idle)
            await DisposeItemAsync(item).ConfigureAwait(false);

        _slots.Dispose();
    }

    private void ReleaseSlot()
    {
        if (Volatile.Read(ref _disposed) != 0)
            return;

        try
        {
            _slots.Release();
        }
        catch (ObjectDisposedException)
        {
            // DisposeAsync won the race after the disposed flag was observed as clear.
        }
    }

    private static void DisposeItem(T item)
    {
        if (item is IDisposable disposable)
            disposable.Dispose();
    }

    private static async ValueTask DisposeItemAsync(T item)
    {
        if (item is IAsyncDisposable asyncDisposable)
            await asyncDisposable.DisposeAsync().ConfigureAwait(false);
        else if (item is IDisposable disposable)
            disposable.Dispose();
    }
}
