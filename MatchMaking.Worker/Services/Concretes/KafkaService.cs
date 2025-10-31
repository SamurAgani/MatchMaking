using Confluent.Kafka;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;
using MatchMaking.Worker.Services.Abstracts;
using System.Text.Json;

namespace MatchMaking.Worker.Services.Concretes
{
    public class KafkaService : IKafkaService, IDisposable
    {
        private readonly IConsumer<string, string> _consumer;
        private readonly IProducer<string, string> _producer;
        private readonly IMatchMakingService _matchMakingService;
        private readonly ILogger<KafkaService> _logger;

        public KafkaService(
            IConfiguration configuration,
            IMatchMakingService matchMakingService,
            ILogger<KafkaService> logger)
        {
            _matchMakingService = matchMakingService;
            _logger = logger;

            var bootstrapServers = configuration["Kafka:BootstrapServers"] ?? "localhost:9092";

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "matchmaking-worker-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            };

            _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = "matchmaking-worker-producer"
            };

            _producer = new ProducerBuilder<string, string>(producerConfig).Build();
        }

        public async Task StartConsumingAsync(CancellationToken cancellationToken)
        {
            _consumer.Subscribe(KafkaTopics.MatchmakingRequest);
            _logger.LogInformation("Started consuming from topic: {Topic}", KafkaTopics.MatchmakingRequest);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await ProcessMessageAsync(cancellationToken);
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

        private async Task ProcessMessageAsync(CancellationToken cancellationToken)
        {
            try
            {
                var consumeResult = _consumer.Consume(cancellationToken);

                if (consumeResult?.Message?.Value == null)
                    return;

                var matchRequest = JsonSerializer.Deserialize<MatchRequest>(consumeResult.Message.Value);

                if (matchRequest == null)
                {
                    _logger.LogWarning("Failed to deserialize match request");
                    return;
                }

                await ProcessMatchRequestAsync(matchRequest, cancellationToken);
                _consumer.Commit(consumeResult);
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Error consuming message from Kafka");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing match request");
            }
        }

        private async Task ProcessMatchRequestAsync(MatchRequest matchRequest, CancellationToken cancellationToken)
        {
            var match = await _matchMakingService.TryCreateMatchAsync(
                matchRequest.UserId,
                cancellationToken);

            if (match == null)
                return;

            await PublishMatchCompleteAsync(match, cancellationToken);
        }

        public async Task PublishMatchCompleteAsync(MatchComplete matchComplete, CancellationToken cancellationToken = default)
        {
            try
            {
                var message = new Message<string, string>
                {
                    Key = matchComplete.MatchId,
                    Value = JsonSerializer.Serialize(matchComplete)
                };

                var result = await _producer.ProduceAsync(
                    KafkaTopics.MatchmakingComplete,
                    message,
                    cancellationToken);

                _logger.LogInformation(
                    "Published match complete for MatchId {MatchId} to topic {Topic} at offset {Offset}. Players: {Players}",
                    matchComplete.MatchId,
                    KafkaTopics.MatchmakingComplete,
                    result.Offset,
                    string.Join(", ", matchComplete.UserIds));
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogError(ex, "Failed to publish match complete for MatchId {MatchId}", matchComplete.MatchId);
                throw;
            }
        }

        public void Dispose()
        {
            _consumer?.Dispose();
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
        }
    }
}
