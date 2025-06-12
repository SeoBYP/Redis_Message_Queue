namespace Redis_Message_Queue
{
    /// <summary>Redis 연결 및 큐 설정 옵션</summary>
    public class RedisOptions
    {
        public string Url { get; set; } = "localhost:6379";
        public string QueueKey { get; set; } = "default-queue";
        public RedisQueueMode Mode { get; set; } = RedisQueueMode.List;
        public string ConsumerGroup { get; set; } = "default-group";
        public string ConsumerId { get; set; } = "consumer-1";
    }
}