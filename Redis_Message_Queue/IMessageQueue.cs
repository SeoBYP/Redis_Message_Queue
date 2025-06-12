

namespace Redis_Message_Queue
{
    /// <summary>
    /// 메시지 큐의 인터페이스 정의
    /// </summary>
    public interface IMessageQueue<T>
    {
        /// <summary>메시지 큐에 메시지 추가</summary>
        Task EnqueueAsync(T message);

        /// <summary>큐에 남아있는 모든 메시지를 비동기로 순차적으로 디큐</summary>
        IAsyncEnumerable<T> DequeueAllAsync(CancellationToken cancellationToken = default);
    }
}
