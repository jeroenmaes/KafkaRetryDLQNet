using Confluent.Kafka;
using KafkaRetryDLQNet.Data;
using KafkaRetryDLQNet.Dto;
using KafkaRetryDLQNet.Kafka;
using KafkaRetryDLQNet.Producer;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace KafkaRetryDLQNet.Consumers;

public class MainConsumer : BackgroundService
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<MainConsumer> _logger;
    private readonly EmployeeRepository _repository;
    private readonly MessageRouter _router;

    public MainConsumer(
        IOptions<KafkaSettings> settings,
        ILogger<MainConsumer> logger,
        EmployeeRepository repository,
        MessageRouter router)
    {
        _settings = settings.Value;
        _logger = logger;
        _repository = repository;
        _router = router;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait a bit for topics to be created
        await Task.Delay(3000, stoppingToken);

        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = $"{_settings.GroupId}-main",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_settings.Topics.Main);

        _logger.LogInformation("MainConsumer started, subscribed to {Topic}", _settings.Topics.Main);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                
                _logger.LogInformation("MainConsumer received message: Key={Key}", result.Message.Key);

                var employeeMessage = JsonSerializer.Deserialize<EmployeeMessage>(result.Message.Value);
                if (employeeMessage == null)
                {
                    _logger.LogError("Failed to deserialize message");
                    consumer.Commit(result);
                    continue;
                }

                try
                {
                    await _repository.UpdateEmployeeAsync(employeeMessage);
                    consumer.Commit(result);
                    _logger.LogInformation("MainConsumer successfully processed message for EmployeeID {EmployeeID}", 
                        employeeMessage.EmployeeID);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "MainConsumer failed to process message for EmployeeID {EmployeeID}", 
                        employeeMessage.EmployeeID);
                    
                    await _router.RouteToRetry1Async(result, ex.Message);
                    consumer.Commit(result);
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Consume error in MainConsumer");
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        consumer.Close();
        _logger.LogInformation("MainConsumer stopped");
    }
}
