using Confluent.Kafka;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace KafkaRetryDLQNet;

public class Retry3Consumer : BackgroundService
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<Retry3Consumer> _logger;
    private readonly EmployeeRepository _repository;
    private readonly MessageRouter _router;

    public Retry3Consumer(
        IOptions<KafkaSettings> settings,
        ILogger<Retry3Consumer> logger,
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
            GroupId = $"{_settings.GroupId}-retry3",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(_settings.Topics.Retry3);

        _logger.LogInformation("Retry3Consumer started, subscribed to {Topic}", _settings.Topics.Retry3);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);
                
                var notBefore = result.Message.Headers.GetLongHeader(HeaderHelper.NotBeforeEpochMs);
                var retryStage = result.Message.Headers.GetIntHeader(HeaderHelper.RetryStage);
                var originTopic = result.Message.Headers.GetStringHeader(HeaderHelper.OriginTopic);

                _logger.LogInformation("Retry3Consumer received message: Key={Key}, RetryStage={RetryStage}, Origin={Origin}", 
                    result.Message.Key, retryStage, originTopic);

                // Wait if message arrived too early
                if (notBefore.HasValue)
                {
                    var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    if (now < notBefore.Value)
                    {
                        var waitMs = (int)(notBefore.Value - now);
                        _logger.LogInformation("Message arrived early. Waiting {WaitMs}ms before processing", waitMs);
                        await Task.Delay(waitMs, stoppingToken);
                    }
                }

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
                    _logger.LogInformation("Retry3Consumer successfully processed message for EmployeeID {EmployeeID}", 
                        employeeMessage.EmployeeID);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Retry3Consumer failed to process message for EmployeeID {EmployeeID} - routing to DLQ", 
                        employeeMessage.EmployeeID);
                    
                    await _router.RouteToDeadLetterAsync(result, ex.Message);
                    consumer.Commit(result);
                }
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Consume error in Retry3Consumer");
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        consumer.Close();
        _logger.LogInformation("Retry3Consumer stopped");
    }
}
