using StackExchange.Redis;

namespace Redis_Message_Queue
{
    public abstract class RedisMessageQueueBase<T> : IMessageQueue<T>
    {
        protected readonly ConnectionMultiplexer _redis;
        protected readonly IDatabase _db;
        protected readonly string _queueKey;

        protected RedisMessageQueueBase(RedisOptions options)
        {
            _redis = ConnectionMultiplexer.Connect(options.Url);
            _db = _redis.GetDatabase();
            _queueKey = options.QueueKey;
        }
        public abstract Task EnqueueAsync(T message);
        public abstract IAsyncEnumerable<T> DequeueAllAsync(CancellationToken cancellationToken = default);

        protected abstract Task<string> SerializeMessage(T message);
        protected abstract Task<T> DeserializeMessage(string data);
    }
}