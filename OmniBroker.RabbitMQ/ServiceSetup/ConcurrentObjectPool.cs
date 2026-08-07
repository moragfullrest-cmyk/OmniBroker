using System.Collections.Concurrent;

namespace OmniBroker.RabbitMQ.ServiceSetup;

public sealed class ConcurrentObjectPool<T> : IAsyncDisposable
    where T : class
{
    private readonly ConcurrentBag<T> _objects = [];
    private readonly Func<T> _objectGenerator;
    private readonly int _maxSize;
    private int _created;

    public ConcurrentObjectPool(Func<T> objectGenerator, int maxSize = 32)
    {
        _objectGenerator = objectGenerator ?? throw new ArgumentNullException(nameof(objectGenerator));
        if (maxSize < 1) throw new ArgumentOutOfRangeException(nameof(maxSize));
        _maxSize = maxSize;
    }

    public T Get()
    {
        while (true)
        {
            if (_objects.TryTake(out T? item))
                return item;

            int created = Volatile.Read(ref _created);
            if (created < _maxSize)
            {
                if (Interlocked.CompareExchange(ref _created, created + 1, created) == created)
                    return _objectGenerator();
                continue;
            }

            Thread.Yield();
        }
    }

    public void Return(T item) => _objects.Add(item);

    public async ValueTask DisposeAsync()
    {
        while (_objects.TryTake(out T? item))
        {
            if (item is IAsyncDisposable asyncDisposable)
                await asyncDisposable.DisposeAsync();
            else if (item is IDisposable disposable)
                disposable.Dispose();
        }
    }
}
