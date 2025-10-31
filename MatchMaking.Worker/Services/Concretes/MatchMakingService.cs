using MatchMaking.Shared.Models;
using MatchMaking.Worker.Services.Abstracts;
using StackExchange.Redis;

namespace MatchMaking.Worker.Services.Concretes
{
    public class MatchMakingService : IMatchMakingService
    {
        private readonly IConnectionMultiplexer _redis;
        private readonly ILogger<MatchMakingService> _logger;
        private readonly int _playersPerMatch;
        private const string WaitingPlayersKey = "waiting_players";
        private static readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        public MatchMakingService(
            IConnectionMultiplexer redis,
            IConfiguration configuration,
            ILogger<MatchMakingService> logger)
        {
            _redis = redis;
            _logger = logger;
            _playersPerMatch = configuration.GetValue<int>("PlayersPerMatch", 3);
        }

        public async Task<MatchComplete?> TryCreateMatchAsync(string userId, CancellationToken cancellationToken = default)
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
}
