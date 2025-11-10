using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;

namespace MatchMaking.Service.BackgroundServices;

public class KafkaBackgroundService(
    ILogger<KafkaBackgroundService> logger,
    IKafkaConsumer<string, MatchComplete> consumer,
    IMatchProcessingService matchProcessingService) : BackgroundService
{
    private readonly ILogger<KafkaBackgroundService> _logger = logger;
    private readonly IKafkaConsumer<string, MatchComplete> _consumer = consumer;
    private readonly IMatchProcessingService _matchProcessingService = matchProcessingService;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka consumer service starting at: {Time}", DateTimeOffset.Now);

        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _consumer.Subscribe(KafkaTopics.MatchmakingComplete);
        _ = Task.Run(async () =>
        {
            try
            {
                await _consumer.StartConsumingAsync(_matchProcessingService.ProcessMatchCompleteMessageAsync, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consumer loop canceled");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in Kafka consumer loop");
            }
        }, linkedCts.Token);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Kafka consumer service stopping at: {Time}", DateTimeOffset.Now);
        await base.StopAsync(cancellationToken);
    }
}