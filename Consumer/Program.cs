// See https://aka.ms/new-console-template for more information
// See https://aka.ms/new-console-template for more information
using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace MyRedisQueue
{
    class Consumer
    {
        static void Main(string[] args)
        {
            // 1) Redis 서버에 연결
            using var redis = ConnectionMultiplexer.Connect("localhost:6379");
            IDatabase db = redis.GetDatabase();

            Console.WriteLine("=== Redis Queue Consumer ===");
            Console.WriteLine("큐에 메시지가 없으면 1초마다 폴링(polling)합니다. Ctrl+C를 누르면 종료합니다.");
    
            string queueKey = "mq:list";

            while (true)
            {
                // 2) 왼쪽에서 하나 꺼내기 (비어 있으면 null 반환)
                //    - Redis 명령어: LPOP mq:list
                var value = db.ListLeftPop(queueKey);
                if (value.HasValue)
                {
                    // 메시지 처리 (예시: 화면에 출력)
                    Console.WriteLine($"  [POPPED] \"{value}\"  (남은 요소 수: {db.ListLength(queueKey)})");
                    // 실제 작업 구현: 가령 제로MQ처럼 별도 작업 호출, DB 쓰기, 파일 로깅 등
                }
                else
                {
                    // 큐가 비어 있으면 잠시 대기 후 폴링
                    Console.WriteLine("  (큐 비어 있음 → 1초 뒤 재시도)");
                    Thread.Sleep(1000);
                }
            }
        }
    }
}
