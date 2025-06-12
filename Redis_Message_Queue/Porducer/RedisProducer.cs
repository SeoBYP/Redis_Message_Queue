namespace Redis_Message_Queue;

/// <summary>
/// 메시지를 큐에 넣는 Producer
/// </summary>
public class RedisProducer<T>
{
    private readonly IMessageQueue<T> _queue;

    public RedisProducer(IMessageQueue<T> queue)
    {
        _queue = queue;
    }

    public Task ProduceAsync(T message)
    { 
        return _queue.EnqueueAsync(message);
    }
}