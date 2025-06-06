// See https://aka.ms/new-console-template for more information
using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace MyRedisQueue
{
    class Producer
    {
        static void Main(string[] args)
        {
            using var redis =  ConnectionMultiplexer.Connect("localhost:6379");
            
            IDatabase db = redis.GetDatabase();
            
            Console.WriteLine("=== Redis Queue Producer ===");
            Console.WriteLine("메시지를 입력하고 Enter를 누르면, Redis 큐에 푸시됩니다.");
            Console.WriteLine("종료하려면 빈 메시지 뒤에 Ctrl+C");

            while (true)
            {
                Console.Write("메세지> ");
                var message = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(message))
                {
                    Console.WriteLine("프로듀서 종료");
                    break;
                }

                string queueKey = "mq:list";
                db.ListRightPush(queueKey, message);
                Console.WriteLine($"  [PUSHED] \"{message}\" (요소 수: {db.ListLength(queueKey)})");
            }
            
            // 연결 종료
            redis.Close();
        }
    }
}
