namespace KafkaRetryDLQNet.Kafka;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string GroupId { get; set; } = "kafka-retry-dlq-demo";
    public KafkaTopics Topics { get; set; } = new();
    public RetryDelays RetryDelays { get; set; } = new();
    public int ProducerIntervalMs { get; set; } = 10000;
}

public class KafkaTopics
{
    public string Main { get; set; } = "main";
    public string Retry1 { get; set; } = "retry-1";
    public string Retry2 { get; set; } = "retry-2";
    public string Retry3 { get; set; } = "retry-3";
    public string DeadLetter { get; set; } = "deadletter";
}

public class RetryDelays
{
    public int Retry1Ms { get; set; } = 5000;
    public int Retry2Ms { get; set; } = 15000;
    public int Retry3BaseMs { get; set; } = 30000;
    public int Retry3JitterMs { get; set; } = 10000;
}
