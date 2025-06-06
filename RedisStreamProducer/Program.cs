using System;
using StackExchange.Redis;

namespace MyRedisStreamQueue
{
    class Producer
    {
        static void Main(string[] args)
        {
            using var redis = ConnectionMultiplexer.Connect("localhost:6379");
            IDatabase db = redis.GetDatabase();

            Console.WriteLine("=== Redis Stream Producer ===");
            Console.WriteLine("메시지를 입력하고 Enter를 누르면, Redis Stream에 추가됩니다.");
            Console.WriteLine("종료하려면 빈 메시지 뒤에 Ctrl+C");

            // 사용할 Stream 키 이름
            string streamKey = "mq:stream";

            while (true)
            {
                Console.Write("메시지> ");
                string message = Console.ReadLine() ?? "";
                
                // 빈 줄 입력 시 종료
                if (string.IsNullOrWhiteSpace(message))
                {
                    Console.WriteLine("프로듀서 종료");
                    break;
                }
                // 2) Stream에 메시지 추가
                //    - 필드1: "message", 값: message
                //    - Stream ID: 자동(*) 지정
                //
                // Redis 명령어 예시: XADD mq:stream * message "안녕하세요"
                //
                var messageId = db.StreamAdd(streamKey, "message", message);
                Console.WriteLine($"  [ADDED] ID={messageId} \"{message}\"");
            }
            
            redis.Close();
        }
    }
}

