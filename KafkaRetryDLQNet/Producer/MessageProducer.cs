using Confluent.Kafka;
using KafkaRetryDLQNet.Dto;
using KafkaRetryDLQNet.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace KafkaRetryDLQNet.Producer;

public class MessageProducer : BackgroundService
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<MessageProducer> _logger;
    private readonly IProducer<string, string> _producer;
    private int _messageCounter = 0;

    public MessageProducer(IOptions<KafkaSettings> settings, ILogger<MessageProducer> logger)
    {
        _settings = settings.Value;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers
        };

        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait a bit for topics to be created
        await Task.Delay(3000, stoppingToken);

        _logger.LogInformation("MessageProducer started. Producing messages every {IntervalMs}ms", 
            _settings.ProducerIntervalMs);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var employeeId = (_messageCounter % 5) + 1; // Cycle through employees 1-5
                var syncTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                
                var message = new EmployeeMessage
                {
                    EmployeeID = employeeId,
                    FirstName = $"Name_{_messageCounter}",
                    SyncTime = syncTime
                };

                var json = JsonSerializer.Serialize(message);
                var key = employeeId.ToString();

                var kafkaMessage = new Message<string, string>
                {
                    Key = key,
                    Value = json
                };

                await _producer.ProduceAsync(_settings.Topics.Main, kafkaMessage, stoppingToken);

                _logger.LogInformation("Produced message #{Counter} to {Topic}: EmployeeID={EmployeeID}, FirstName={FirstName}",
                    _messageCounter, _settings.Topics.Main, message.EmployeeID, message.FirstName);

                _messageCounter++;
                await Task.Delay(_settings.ProducerIntervalMs, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error producing message");
                await Task.Delay(1000, stoppingToken);
            }
        }

        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
    }
}
