using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Redis_Message_Queue
{
    /// <summary>
    /// Redis List 구조를 이용한 메시지 큐 구현
    /// </summary>
    public class RedisListMessageQueue<T> : RedisMessageQueueBase<T>, IMessageQueue<T>
    {
        public RedisListMessageQueue(RedisOptions options) : base(options)
        {
        }

        public override async Task EnqueueAsync(T message)
        {
            var payload = await SerializeMessage(message);
            await _db.ListRightPushAsync(_queueKey, payload);
        }

        public override async IAsyncEnumerable<T> DequeueAllAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var data = await _db.ListLeftPopAsync(_queueKey);
                if (data.IsNullOrEmpty) yield break;
                yield return await DeserializeMessage(data);
            }
        }

        protected override Task<string> SerializeMessage(T message)
        {
            return Task.FromResult(JsonSerializer.Serialize(message));
        }

        protected override Task<T> DeserializeMessage(string data)
        {
            return Task.FromResult(JsonSerializer.Deserialize<T>(data)!);
        }
    }
}