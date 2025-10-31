using MatchMaking.Worker.Services;
using MatchMaking.Worker.Services.Abstracts;

namespace MatchMaking.Worker
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IKafkaService _kafkaService;

        public Worker(ILogger<Worker> logger, IKafkaService kafkaService)
        {
            _logger = logger;
            _kafkaService = kafkaService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("MatchMaking Worker starting at: {Time}", DateTimeOffset.Now);

            try
            {
                await _kafkaService.StartConsumingAsync(stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in MatchMaking Worker");
                throw;
            }
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("MatchMaking Worker stopping at: {Time}", DateTimeOffset.Now);
            await base.StopAsync(cancellationToken);
        }
    }
}
