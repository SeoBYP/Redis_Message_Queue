// See https://aka.ms/new-console-template for more information

using System;
using System.Threading.Tasks;
using Redis_Message_Queue;
using StackExchange.Redis;

namespace MyRedisQueue
{
    class Producer
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
            IMessageQueue<MyMessage> queue = new RedisStreamMessageQueue<MyMessage>(options);
            var producer = new RedisProducer<MyMessage>(queue);

            Console.WriteLine("=== Redis Queue Producer ===");
            Console.WriteLine("메시지를 입력하고 Enter를 누르면, Redis 큐에 푸시됩니다.");
            Console.WriteLine("종료하려면 빈 메시지를 입력하세요.");

            while (true)
            {
                Console.Write("메시지> ");
                var input = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(input))
                {
                    Console.WriteLine("Producer 종료");
                    break;
                }

                var msg = new MyMessage(input, DateTime.UtcNow);
                await producer.ProduceAsync(msg);
                Console.WriteLine($"Sent: {msg.Content} at {msg.Timestamp}");
            }
        }
    }
}