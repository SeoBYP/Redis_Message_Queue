namespace Redis_Message_Queue;

/// <summary>
/// 큐에서 메시지를 읽어 처리하는 Consumer
/// </summary>
public class RedisConsumer<T>
{
    private readonly IMessageQueue<T> _queue;

    // 폴링 간격
    private readonly TimeSpan _pollInterval = TimeSpan.FromSeconds(1);

    public RedisConsumer(IMessageQueue<T> queue)
    {
        _queue = queue;
    }

    public async Task ConsumeAsync(Func<T, Task> handleMessage,
        CancellationToken cancellationToken = default)
    {
        // 프로세스 종료 전까지 계속 폴링
        while (!cancellationToken.IsCancellationRequested)
        {
            var receivedAny = false;
            await ProcessMessagesAsync(handleMessage, cancellationToken);

            if (!receivedAny)
            {
                // 메시지 없으면 폴링 간격만큼 대기
                await Task.Delay(_pollInterval, cancellationToken);
            }
        }
    }

    private async Task<bool> ProcessMessagesAsync(Func<T, Task> handleMessage, CancellationToken cancellationToken)
    {
        var receivedAny = false;
        // DequeueAllAsync가 비어있으면 바로 끝나니,
        // yield된 메시지가 있으면 처리하고, 없으면 딜레이
        await foreach (var msg in _queue.DequeueAllAsync(cancellationToken))
        {
            receivedAny = true;
            try
            {
                await handleMessage(msg);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error processing message: {ex}");
            }
        }

        return receivedAny;
    }
}