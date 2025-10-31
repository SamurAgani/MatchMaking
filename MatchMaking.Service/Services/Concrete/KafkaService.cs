using Confluent.Kafka;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;
using System.Text.Json;

namespace MatchMaking.Service.Services.Concrete
{
    public class KafkaService : IKafkaService, IDisposable
    {
        private readonly IProducer<string, string> _producer;
        private readonly IConsumer<string, string> _consumer;
        private readonly IMatchStorageService _matchStorageService;
        private readonly ILogger<KafkaService> _logger;

        public KafkaService(
            IConfiguration configuration,
            IMatchStorageService matchStorageService,
            ILogger<KafkaService> logger)
        {
            _matchStorageService = matchStorageService;
            _logger = logger;

            var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = "matchmaking-service-producer"
            };

            _producer = new ProducerBuilder<string, string>(producerConfig).Build();

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "matchmaking-service-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            };

            _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        }

        public async Task PublishMatchRequestAsync(MatchRequest matchRequest, CancellationToken cancellationToken = default)
        {
            try
            {
                var message = new Message<string, string>
                {
                    Key = matchRequest.UserId,
                    Value = JsonSerializer.Serialize(matchRequest)
                };

                var result = await _producer.ProduceAsync(KafkaTopics.MatchmakingRequest, message, cancellationToken);

                _logger.LogInformation(
                    "Published match request for user {UserId} to topic {Topic} at offset {Offset}",
                    matchRequest.UserId,
                    KafkaTopics.MatchmakingRequest,
                    result.Offset);
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogError(ex, "Failed to publish match request for user {UserId}", matchRequest.UserId);
                throw;
            }
        }

        public async Task StartConsumingAsync(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(KafkaTopics.MatchmakingComplete);
            _logger.LogInformation("Started consuming from topic: {Topic}", KafkaTopics.MatchmakingComplete);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await ProcessMessageAsync(stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consumer stopped");
            }
            finally
            {
                _consumer.Close();
            }
        }

        private async Task ProcessMessageAsync(CancellationToken stoppingToken)
        {
            try
            {
                var consumeResult = _consumer.Consume(stoppingToken);

                if (consumeResult?.Message?.Value == null)
                    return;

                var matchComplete = JsonSerializer.Deserialize<MatchComplete>(consumeResult.Message.Value);

                if (matchComplete == null)
                {
                    _logger.LogWarning("Failed to deserialize match complete message");
                    return;
                }

                await ProcessMatchCompleteAsync(matchComplete, stoppingToken);
                _consumer.Commit(consumeResult);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Error consuming message from Kafka");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing match complete message");
            }
        }

        private async Task ProcessMatchCompleteAsync(MatchComplete matchComplete, CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "Processing match {MatchId} with {PlayerCount} players",
                matchComplete.MatchId,
                matchComplete.UserIds.Count);

            foreach (var userId in matchComplete.UserIds)
            {
                await StoreMatchForUserAsync(userId, matchComplete, stoppingToken);
            }
        }

        private async Task StoreMatchForUserAsync(string userId, MatchComplete matchComplete, CancellationToken stoppingToken)
        {
            try
            {
                await _matchStorageService.StoreMatchForUserAsync(
                    userId,
                    matchComplete,
                    stoppingToken);

                _logger.LogInformation(
                    "Stored match {MatchId} for user {UserId}",
                    matchComplete.MatchId,
                    userId);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to store match {MatchId} for user {UserId}",
                    matchComplete.MatchId,
                    userId);
            }
        }

        public void Dispose()
        {
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
            _consumer?.Dispose();
        }
    }
}
