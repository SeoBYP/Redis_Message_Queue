using System;
using StackExchange.Redis;

namespace MyRedisStreamQueue
{
    static class RedisStreamUtils
    {
        /// <summary>
        /// StreamKey에 Consumer Group이 없으면 생성합니다. 이미 존재하면 예외를 무시합니다.
        /// </summary>
        public static void EnsureConsumerGroup(IDatabase db, string streamKey, string groupName)
        {
            try
            {
                // Stream이 없을 때도 만들려면 MKSTREAM 옵션(true)을 함께 전달
                db.StreamCreateConsumerGroup(streamKey, groupName,StreamPosition.NewMessages, createStream:true);
                Console.WriteLine($"[GROUP CREATED] '{groupName}' for Stream '{streamKey}'");
            }
            catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
            {
                // 이미 같은 이름의 Consumer Group이 있으면 "BUSYGROUP" 예외 발생
                Console.WriteLine($"[GROUP EXISTS] '{groupName}' already exists for Stream '{streamKey}'");
            }
        }
    }

    class Consumer
    {
        // Consumer Group 및 Consumer 이름 정의
        private const string StreamKey = "mq:stream";
        private const string GroupName = "mq-group";
        private static string ConsumerName => Environment.MachineName + "-" + Guid.NewGuid().ToString("N").Substring(0, 6);
        // ex) "MyMac-3f5a1b"

        static void Main(string[] args)
        {
            // 1) Redis 연결
            var redis = ConnectionMultiplexer.Connect("localhost:6379");
            IDatabase db = redis.GetDatabase();
            
            // 2) Consumer Group 생성 (없으면 만들고, 이미 있으면 무시)
            RedisStreamUtils.EnsureConsumerGroup(db, StreamKey, GroupName);
            
            Console.WriteLine("=== Redis Stream Consumer ===");
            Console.WriteLine($"Consumer '{ConsumerName}' in Group '{GroupName}' is running...");
            Console.WriteLine("Ctrl+C로 종료하세요.");
            
            // Loop: 블록킹 XREADGROUP 방식 → 메시지 도착 시 즉시 처리
            // COUNT: 한 번에 최대 몇 건 가져올지 지정 (예: 1~10 사이)
            // BLOCK: millisecond 단위, 0으로 설정하면 무한 대기 => 비동기로 작성시
            const int FETCH_COUNT = 5;
            const int sleep = 1000;  // 1초 동안 대기했다가 새 메시지 없으면 리턴

            while (true)
            {
                try
                {
                    // 3) XREADGROUP 호출: 아직 어느 컨슈머에도 전달되지 않은('>') 새 메시지 읽기
                    //    - 반환 타입: StreamEntry[][]
                    //    - StreamPositions 배열을 한 개만 넘김: [ new StreamPosition(StreamKey, ">") ]
                    var entries = db.StreamReadGroup(
                        StreamKey,
                        GroupName,
                        ConsumerName,
                        StreamPosition.NewMessages,
                        count: FETCH_COUNT);

                    if (entries.Length > 0)
                    {
                        foreach (var entry in entries)
                        {
                            // entry.Id: 메시지 ID (예: "1625352510919-0")
                            // entry.Values: KeyValuePair<RedisValue, RedisValue>[] (필드-값 배열)
                            var messageId = entry.Id;
                            var values = entry.Values;
                            
                            string? payload = values.FirstOrDefault(kv => kv.Name == "message").Value;

                            Console.WriteLine($"[CONSUME] ID={messageId} Payload=\"{payload}\"");
                            
                            // 4) 처리 완료 후 ACK 보내기
                            db.StreamAcknowledge(StreamKey, GroupName, messageId);
                            Console.WriteLine($"  [ACK] ID={messageId}");
                        }
                    }
                    else
                    {
                        // block timeout 또는 메시지 없음
                        Console.WriteLine("  (새 메시지 없음 → 다시 XREADGROUP)");
                        Thread.Sleep(sleep);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[ERROR] {ex.Message}");
                    Thread.Sleep(sleep);
                }
            }
            
        }
    }
}

