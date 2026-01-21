using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace KafkaRetryDLQNet;

public class MessageRouter
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<MessageRouter> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly Random _random = new();

    public MessageRouter(IOptions<KafkaSettings> settings, ILogger<MessageRouter> logger)
    {
        _settings = settings.Value;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task RouteToRetry1Async(ConsumeResult<string, string> originalMessage, string errorMessage)
    {
        var notBefore = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + _settings.RetryDelays.Retry1Ms;
        var originTopic = originalMessage.Message.Headers.GetStringHeader(HeaderHelper.OriginTopic) 
            ?? originalMessage.Topic;

        var headers = new Headers
        {
            { HeaderHelper.RetryStage, BitConverter.GetBytes(1) },
            { HeaderHelper.NotBeforeEpochMs, BitConverter.GetBytes(notBefore) },
            { HeaderHelper.OriginTopic, System.Text.Encoding.UTF8.GetBytes(originTopic) },
            { HeaderHelper.LastError, System.Text.Encoding.UTF8.GetBytes(errorMessage) }
        };

        await ProduceAsync(_settings.Topics.Retry1, originalMessage.Message.Key, originalMessage.Message.Value, headers);
        
        _logger.LogWarning("Routed message to {Topic} with delay {DelayMs}ms. Origin: {OriginTopic}, Error: {Error}",
            _settings.Topics.Retry1, _settings.RetryDelays.Retry1Ms, originTopic, errorMessage);
    }

    public async Task RouteToRetry2Async(ConsumeResult<string, string> originalMessage, string errorMessage)
    {
        var notBefore = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + _settings.RetryDelays.Retry2Ms;
        var originTopic = originalMessage.Message.Headers.GetStringHeader(HeaderHelper.OriginTopic) 
            ?? originalMessage.Topic;

        var headers = new Headers
        {
            { HeaderHelper.RetryStage, BitConverter.GetBytes(2) },
            { HeaderHelper.NotBeforeEpochMs, BitConverter.GetBytes(notBefore) },
            { HeaderHelper.OriginTopic, System.Text.Encoding.UTF8.GetBytes(originTopic) },
            { HeaderHelper.LastError, System.Text.Encoding.UTF8.GetBytes(errorMessage) }
        };

        await ProduceAsync(_settings.Topics.Retry2, originalMessage.Message.Key, originalMessage.Message.Value, headers);
        
        _logger.LogWarning("Routed message to {Topic} with delay {DelayMs}ms. Origin: {OriginTopic}, Error: {Error}",
            _settings.Topics.Retry2, _settings.RetryDelays.Retry2Ms, originTopic, errorMessage);
    }

    public async Task RouteToRetry3Async(ConsumeResult<string, string> originalMessage, string errorMessage)
    {
        var jitter = _random.Next(0, _settings.RetryDelays.Retry3JitterMs);
        var totalDelay = _settings.RetryDelays.Retry3BaseMs + jitter;
        var notBefore = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + totalDelay;
        var originTopic = originalMessage.Message.Headers.GetStringHeader(HeaderHelper.OriginTopic) 
            ?? originalMessage.Topic;

        var headers = new Headers
        {
            { HeaderHelper.RetryStage, BitConverter.GetBytes(3) },
            { HeaderHelper.NotBeforeEpochMs, BitConverter.GetBytes(notBefore) },
            { HeaderHelper.OriginTopic, System.Text.Encoding.UTF8.GetBytes(originTopic) },
            { HeaderHelper.LastError, System.Text.Encoding.UTF8.GetBytes(errorMessage) }
        };

        await ProduceAsync(_settings.Topics.Retry3, originalMessage.Message.Key, originalMessage.Message.Value, headers);
        
        _logger.LogWarning("Routed message to {Topic} with delay {DelayMs}ms (base: {BaseMs}, jitter: {JitterMs}). Origin: {OriginTopic}, Error: {Error}",
            _settings.Topics.Retry3, totalDelay, _settings.RetryDelays.Retry3BaseMs, jitter, originTopic, errorMessage);
    }

    public async Task RouteToDeadLetterAsync(ConsumeResult<string, string> originalMessage, string errorMessage)
    {
        var originTopic = originalMessage.Message.Headers.GetStringHeader(HeaderHelper.OriginTopic) 
            ?? originalMessage.Topic;
        var retryStage = originalMessage.Message.Headers.GetIntHeader(HeaderHelper.RetryStage) ?? 0;

        var headers = new Headers
        {
            { HeaderHelper.RetryStage, BitConverter.GetBytes(retryStage) },
            { HeaderHelper.OriginTopic, System.Text.Encoding.UTF8.GetBytes(originTopic) },
            { HeaderHelper.LastError, System.Text.Encoding.UTF8.GetBytes(errorMessage) }
        };

        await ProduceAsync(_settings.Topics.DeadLetter, originalMessage.Message.Key, originalMessage.Message.Value, headers);
        
        _logger.LogError("Routed message to DLQ. Origin: {OriginTopic}, RetryStage: {RetryStage}, Error: {Error}",
            originTopic, retryStage, errorMessage);
    }

    private async Task ProduceAsync(string topic, string key, string value, Headers headers)
    {
        var message = new Message<string, string>
        {
            Key = key,
            Value = value,
            Headers = headers
        };

        await _producer.ProduceAsync(topic, message);
    }
}
