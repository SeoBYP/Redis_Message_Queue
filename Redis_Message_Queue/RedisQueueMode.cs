namespace Redis_Message_Queue
{
    /// <summary>Redis 메시지 큐 동작 모드 (List or Stream)</summary>
    public enum RedisQueueMode
    {
        List,
        Stream
    }
}