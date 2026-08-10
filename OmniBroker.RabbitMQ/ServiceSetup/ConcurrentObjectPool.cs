using System.Collections.Concurrent;

namespace OmniBroker.RabbitMQ.ServiceSetup;

public sealed class ConcurrentObjectPool<T> : IAsyncDisposable
    where T : class
{
    private readonly ConcurrentBag<T> _objects = [];
    private readonly Func<CancellationToken, Task<T>> _objectGenerator;
    private readonly int _maxSize;
    private readonly SemaphoreSlim _signal = new(0);
    private int _created;
    private int _disposed;

    public ConcurrentObjectPool(Func<CancellationToken, Task<T>> objectGenerator, int maxSize = 32)
    {
        _objectGenerator = objectGenerator ?? throw new ArgumentNullException(nameof(objectGenerator));
        if (maxSize < 1) throw new ArgumentOutOfRangeException(nameof(maxSize));
        _maxSize = maxSize;
    }

    public async Task<T> GetAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

            if (_objects.TryTake(out T? item))
                return item;

            int created = Volatile.Read(ref _created);
            if (created < _maxSize)
            {
                if (Interlocked.CompareExchange(ref _created, created + 1, created) == created)
                {
                    try
                    {
                        return await _objectGenerator(cancellationToken).ConfigureAwait(false);
                    }
                    catch
                    {
                        Interlocked.Decrement(ref _created);
                        if (Volatile.Read(ref _disposed) == 0)
                            _signal.Release();
                        throw;
                    }
                }

                continue;
            }

            await _signal.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public void Return(T item)
    {
        ArgumentNullException.ThrowIfNull(item);
        if (Volatile.Read(ref _disposed) != 0)
        {
            DisposeItem(item);
            return;
        }

        _objects.Add(item);
        _signal.Release();
    }

    /// <summary>
    /// Drop a broken item and free a pool slot so a new one can be created.
    /// </summary>
    public async ValueTask DiscardAsync(T item)
    {
        ArgumentNullException.ThrowIfNull(item);
        await DisposeItemAsync(item).ConfigureAwait(false);
        Interlocked.Decrement(ref _created);
        if (Volatile.Read(ref _disposed) == 0)
            _signal.Release();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        while (_objects.TryTake(out T? item))
            await DisposeItemAsync(item).ConfigureAwait(false);

        _signal.Dispose();
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
