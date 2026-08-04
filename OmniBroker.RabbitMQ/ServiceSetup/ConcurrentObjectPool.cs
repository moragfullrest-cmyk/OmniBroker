using System.Collections.Concurrent;

namespace OmniBroker.RabbitMQ.ServiceSetup;

public class ConcurrentObjectPool<T>(Func<T> objectGenerator)
{
    private readonly ConcurrentBag<T> _objects = [];
    private readonly Func<T> _objectGenerator = objectGenerator ?? throw new ArgumentNullException(nameof(objectGenerator));

    public T Get() => _objects.TryTake(out T item) ? item : _objectGenerator();

    public void Return(T item) => _objects.Add(item);
}
