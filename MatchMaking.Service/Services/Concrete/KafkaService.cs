using Confluent.Kafka;
using FluentResults;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;
using System.Text.Json;

namespace MatchMaking.Service.Services.Concrete
{
    public class KafkaService : IKafkaService
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

        public async Task<Result> PublishMatchRequestAsync(MatchRequest matchRequest, CancellationToken cancellationToken = default)
        {
            try
            {
                var value = JsonSerializer.Serialize(matchRequest);
                var message = new Message<string, string> { Key = matchRequest.UserId, Value = value };

                var result = await _producer.ProduceAsync(KafkaTopics.MatchmakingRequest, message, cancellationToken);

                _logger.LogInformation(
                    "Published match request for user {UserId} to topic {Topic} at offset {Offset}",
                    matchRequest.UserId,
                    KafkaTopics.MatchmakingRequest,
                    result.Offset);

                return Result.Ok();
            }
            catch (ProduceException<string, string> ex)
            {
                _logger.LogError(ex, "Failed to publish match request for user {UserId}", matchRequest.UserId);
                return Result.Fail("Failed to publish match request");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unexpected error while publishing match request for user {UserId}", matchRequest.UserId);
                return Result.Fail("Unexpected error while publishing match request");
            }
        }

        public async Task<Result> StartConsumingAsync(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(KafkaTopics.MatchmakingComplete);
            _logger.LogInformation("Started consuming from topic: {Topic}", KafkaTopics.MatchmakingComplete);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var processResult = await ProcessMessageAsync(stoppingToken);

                    if (processResult.IsFailed)
                    {
                        _logger.LogWarning("Processing message failed: {Errors}", string.Join(" | ", processResult.Errors.Select(e => e.Message)));
                    }
                }

                return Result.Ok();
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka consumer stopped via cancellation.");
                return Result.Ok();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Fatal error in Kafka consumer loop");
                return Result.Fail("Fatal error in Kafka consumer loop");
            }
            finally
            {
                _consumer.Close();
            }
        }

        private async Task<Result> ProcessMessageAsync(CancellationToken stoppingToken)
        {
            try
            {
                var consumeResult = _consumer.Consume(stoppingToken);
                if (consumeResult?.Message?.Value == null)
                {
                    return Result.Ok();
                }

                MatchComplete? matchComplete;
             
                matchComplete = JsonSerializer.Deserialize<MatchComplete>(consumeResult.Message.Value);

                if (matchComplete is null)
                    return Result.Fail("MatchComplete was null after deserialization");

                var processResult = await ProcessMatchCompleteAsync(matchComplete, stoppingToken);
                if (processResult.IsSuccess)
                {
                    _consumer.Commit(consumeResult);
                }
                else
                {
                    _logger.LogWarning("Processing MatchComplete failed; message left uncommitted to allow retry.");
                }

                return processResult;
            }
            catch (ConsumeException ex)
            {
                _logger.LogError(ex, "Error consuming message from Kafka");
                return Result.Fail("Kafka consume exception");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing match complete message");
                return Result.Fail("Unexpected error processing message");
            }
        }

        private async Task<Result> ProcessMatchCompleteAsync(MatchComplete matchComplete, CancellationToken stoppingToken)
        {
            _logger.LogInformation(
                "Processing match {MatchId} with {PlayerCount} players",
                matchComplete.MatchId,
                matchComplete.UserIds.Count);

            var failures = new List<IError>();

            foreach (var userId in matchComplete.UserIds)
            {
                var storeResult = await StoreMatchForUserAsync(userId, matchComplete, stoppingToken);
                if (storeResult.IsFailed)
                {
                    failures.AddRange(storeResult.Errors);
                }
            }

            return failures.Count == 0
                ? Result.Ok()
                : Result.Fail(failures);
        }

        private async Task<Result> StoreMatchForUserAsync(string userId, MatchComplete matchComplete, CancellationToken stoppingToken)
        {
            var store = await _matchStorageService.StoreMatchForUserAsync(userId, matchComplete, stoppingToken);

            if (store.IsSuccess)
            {
                _logger.LogInformation(
                    "Stored match {MatchId} for user {UserId}",
                    matchComplete.MatchId,
                    userId);
                return Result.Ok();
            }

            foreach (var err in store.Errors)
            {
                _logger.LogError("Failed to store match {MatchId} for user {UserId}: {Error}", matchComplete.MatchId, userId, err.Message);
            }

            return store;
        }

        public void Dispose()
        {
            try
            {
                _producer?.Flush(TimeSpan.FromSeconds(10));
            }
            catch { }

            _producer?.Dispose();
            _consumer?.Dispose();
        }
    }
}
