using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;

namespace KafkaRetryDLQNet;

public class TopicCreator : IHostedService
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<TopicCreator> _logger;

    public TopicCreator(IOptions<KafkaSettings> settings, ILogger<TopicCreator> logger)
    {
        _settings = settings.Value;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = _settings.BootstrapServers
        };

        using var adminClient = new AdminClientBuilder(config).Build();

        var topics = new[]
        {
            _settings.Topics.Main,
            _settings.Topics.Retry1,
            _settings.Topics.Retry2,
            _settings.Topics.Retry3,
            _settings.Topics.DeadLetter
        };

        try
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            var existingTopics = metadata.Topics.Select(t => t.Topic).ToHashSet();

            var topicsToCreate = topics
                .Where(t => !existingTopics.Contains(t))
                .Select(t => new TopicSpecification
                {
                    Name = t,
                    NumPartitions = 1,
                    ReplicationFactor = 1
                })
                .ToList();

            if (topicsToCreate.Any())
            {
                _logger.LogInformation("Creating {Count} topics: {Topics}", 
                    topicsToCreate.Count, string.Join(", ", topicsToCreate.Select(t => t.Name)));
                
                await adminClient.CreateTopicsAsync(topicsToCreate);
                
                _logger.LogInformation("Topics created successfully");
            }
            else
            {
                _logger.LogInformation("All topics already exist");
            }
        }
        catch (CreateTopicsException ex)
        {
            _logger.LogError(ex, "Error creating topics");
            throw;
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
