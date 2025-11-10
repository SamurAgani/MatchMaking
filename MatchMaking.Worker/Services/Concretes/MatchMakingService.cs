using Confluent.Kafka;
using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;
using MatchMaking.Worker.Services.Abstracts;
using StackExchange.Redis;

namespace MatchMaking.Worker.Services.Concretes;

public class MatchMakingService(
    IConnectionMultiplexer redis,
    IConfiguration configuration,
    ILogger<MatchMakingService> logger,
    IKafkaProducer<string, MatchComplete> producer) : IMatchMakingService
{
    private readonly IConnectionMultiplexer _redis = redis;
    private readonly ILogger<MatchMakingService> _logger = logger;
    private readonly int _playersPerMatch = configuration.GetValue<int>("PlayersPerMatch", 3);
    private const string WaitingPlayersKey = "waiting_players";
    private static readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
    private readonly IKafkaProducer<string, MatchComplete> _producer = producer;

    public async Task<bool> ProcessMatchRequestAsync(
           ConsumeResult<string, MatchRequest> consumeResult,
           CancellationToken cancellationToken)
    {
        try
        {
            var matchRequest = consumeResult.Message.Value;

            if (matchRequest == null)
            {
                _logger.LogWarning("Failed to deserialize match request");
                return false;
            }

            var match = await TryCreateMatchAsync(
                matchRequest.UserId,
                cancellationToken);

            if (match != null)
            {
                await PublishMatchCompleteAsync(match, cancellationToken);
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing match request");
            return false;
        }
    }

    private async Task PublishMatchCompleteAsync(MatchComplete matchComplete, CancellationToken cancellationToken)
    {
        try
        {
            var result = await _producer.ProduceAsync(
                KafkaTopics.MatchmakingComplete,
                matchComplete.MatchId,
                matchComplete,
                cancellationToken);

            _logger.LogInformation(
                "Published match complete for MatchId {MatchId} to topic {Topic} at offset {Offset}. Players: {Players}",
                matchComplete.MatchId,
                KafkaTopics.MatchmakingComplete,
                result.Offset,
                string.Join(", ", matchComplete.UserIds));
        }
        catch (ProduceException<string, MatchComplete> ex)
        {
            _logger.LogError(ex, "Failed to publish match complete for MatchId {MatchId}", matchComplete.MatchId);
            throw;
        }
    }

    private async Task<MatchComplete?> TryCreateMatchAsync(string userId, CancellationToken cancellationToken)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            var db = _redis.GetDatabase();

            var waitingPlayers = await db.ListRangeAsync(WaitingPlayersKey);
            if (waitingPlayers.Any(player => player.ToString() == userId))
            {
                _logger.LogWarning("User {UserId} is already in the waiting queue. Skipping duplicate.", userId);
                return null;
            }

            await db.ListRightPushAsync(WaitingPlayersKey, userId);
            _logger.LogInformation("User {UserId} added to waiting players", userId);

            var waitingCount = await db.ListLengthAsync(WaitingPlayersKey);
            _logger.LogDebug("Current waiting players count: {Count}", waitingCount);

            if (waitingCount >= _playersPerMatch)
            {
                var players = new List<string>();
                for (int i = 0; i < _playersPerMatch; i++)
                {
                    var player = await db.ListLeftPopAsync(WaitingPlayersKey);
                    if (player.HasValue)
                    {
                        players.Add(player.ToString());
                    }
                }

                if (players.Count == _playersPerMatch)
                {
                    var matchId = Guid.NewGuid().ToString();
                    var match = new MatchComplete(matchId, players);

                    _logger.LogInformation(
                        "Match created: {MatchId} with players: {Players}",
                        matchId,
                        string.Join(", ", players));

                    return match;
                }
                else
                {
                    _logger.LogWarning(
                        "Failed to get enough players. Expected {Expected}, got {Actual}",
                        _playersPerMatch,
                        players.Count);

                    foreach (var player in players)
                    {
                        await db.ListLeftPushAsync(WaitingPlayersKey, player);
                    }
                }
            }

            return null;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in match creation for user {UserId}", userId);
            throw;
        }
        finally
        {
            _lock.Release();
        }
    }
}
