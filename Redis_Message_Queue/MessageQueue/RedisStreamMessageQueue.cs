using System.Runtime.CompilerServices;
using System.Text.Json;
using StackExchange.Redis;

namespace Redis_Message_Queue
{
    /// <summary>
    /// Redis Stream 구조를 이용한 메시지 큐 구현
    /// </summary>
    public partial class RedisStreamMessageQueue<T> : RedisMessageQueueBase<T>
    {
        private readonly string _consumerGroup;
        private readonly string _consumerId;

        public RedisStreamMessageQueue(RedisOptions options) : base(options)
        {
            _consumerGroup = options.ConsumerGroup;
            _consumerId = options.ConsumerId;
            // 그룹이 없으면 생성
            try
            {
                _db.StreamCreateConsumerGroup(_queueKey, _consumerGroup, StreamPosition.NewMessages);
            }
            catch
            {
            }
        }

        public override async Task EnqueueAsync(T message)
        {
            var payload = await SerializeMessage(message);
            await _db.StreamAddAsync(_queueKey, [new NameValueEntry("data", payload)]);
        }

        public override async IAsyncEnumerable<T> DequeueAllAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var entries = await _db.StreamReadGroupAsync(
                    key: _queueKey,
                    groupName: _consumerGroup,
                    consumerName: _consumerId,
                    count: 10);

                if (entries.Length == 0) yield break;

                foreach (var entry in entries)
                {
                    var data = entry.Values[0].Value.ToString();
                    yield return await DeserializeMessage(data);
                    await _db.StreamAcknowledgeAsync(_queueKey, _consumerGroup, entry.Id);
                }
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