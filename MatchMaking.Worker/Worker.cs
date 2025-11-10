using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;
using MatchMaking.Worker.Services.Abstracts;

namespace MatchMaking.Worker
{
    public class Worker(
        ILogger<Worker> logger,
        IKafkaConsumer<string, MatchRequest> consumer,
        IMatchMakingService matchMakingService) : BackgroundService
    {
        private readonly ILogger<Worker> _logger = logger;
        private readonly IKafkaConsumer<string, MatchRequest> _consumer = consumer;
        private readonly IMatchMakingService _matchMakingService = matchMakingService;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("MatchMaking Worker starting at: {Time}", DateTimeOffset.Now);

            _consumer.Subscribe(KafkaTopics.MatchmakingRequest);
            var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            _ = Task.Run(async () =>
                {
                    try
                    {
                        await _consumer.StartConsumingAsync(_matchMakingService.ProcessMatchRequestAsync, stoppingToken);
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
            _logger.LogInformation("MatchMaking Worker stopping at: {Time}", DateTimeOffset.Now);
            await base.StopAsync(cancellationToken);
        }
    }
}
