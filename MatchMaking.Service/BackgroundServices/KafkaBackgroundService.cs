using System;
using MatchMaking.Service.Services.Abstracts;

namespace MatchMaking.Service.BackgroundServices;

public class KafkaBackgroundService : BackgroundService
{
    private readonly ILogger<KafkaBackgroundService> _logger;
    private readonly IKafkaService _kafkaService;

    public KafkaBackgroundService(ILogger<KafkaBackgroundService> logger, IKafkaService kafkaService)
    {
        _logger = logger;
        _kafkaService = kafkaService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("MatchMaking Service starting at: {Time}", DateTimeOffset.Now);

        try
        {
            _ = Task.Run(() => _kafkaService.StartConsumingAsync(stoppingToken), stoppingToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error in MatchMaking Service");
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("MatchMaking Service stopping at: {Time}", DateTimeOffset.Now);
        await base.StopAsync(cancellationToken);
    }
}