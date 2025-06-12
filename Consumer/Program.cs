// See https://aka.ms/new-console-template for more information
// See https://aka.ms/new-console-template for more information

using System;
using System.Threading.Tasks;
using Redis_Message_Queue;
using StackExchange.Redis;

namespace MyRedisQueue
{
    class Consumer
    {
        public record MyMessage(string Content, DateTime Timestamp);

        static async Task Main(string[] args)
        {
            var options = new RedisOptions
            {
                Url = "localhost:6379",
                QueueKey = "my-queue",
                Mode = RedisQueueMode.List
            };
            // 1) Redis 서버에 연결
            IMessageQueue<MyMessage> queue = new RedisStreamMessageQueue<MyMessage>(options);

            var consumer = new RedisConsumer<MyMessage>(queue);
            Console.WriteLine("=== Redis Queue Consumer ===");
            Console.WriteLine("큐에 메시지가 없으면 1초마다 폴링(polling)합니다. Ctrl+C를 누르면 종료합니다.");
            var cts = new CancellationTokenSource();

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
                Console.WriteLine("취소 요청 받음, 종료 중...");
            };

            try
            {
                // 이 await가 반환될 때까지 계속 메시지를 폴링/처리합니다.
                await consumer.ConsumeAsync(msg =>
                {
                    Console.WriteLine($"Received: {msg.Content} at {msg.Timestamp}");
                    return Task.CompletedTask;
                }, cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Ctrl+C로 취소됐을 때 정상 흐름
            }

            Console.WriteLine("Consumer 종료");
        }
    }
}