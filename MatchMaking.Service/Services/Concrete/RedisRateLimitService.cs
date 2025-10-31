using MatchMaking.Service.Services.Abstracts;
using StackExchange.Redis;

namespace MatchMaking.Service.Services.Concrete
{
    public class RedisRateLimitService : IRateLimitService
    {
        private readonly IConnectionMultiplexer _redis;
        private readonly ILogger<RedisRateLimitService> _logger;
        private readonly int _minIntervalMs;
        private const string WaitingPlayersKey = "waiting_players";

        public RedisRateLimitService(
            IConnectionMultiplexer redis,
            IConfiguration configuration,
            ILogger<RedisRateLimitService> logger)
        {
            _redis = redis;
            _logger = logger;
            _minIntervalMs = configuration.GetValue<int>("RateLimit:MinIntervalMs", 100);
        }

        public async Task<bool> IsRateLimitedAsync(string userId, CancellationToken cancellationToken = default)
        {
            var db = _redis.GetDatabase();
            var key = $"ratelimit:{userId}";

            try
            {
                var nowTicks = DateTimeOffset.UtcNow.Ticks;

                var lastRequestTicks = await db.StringGetAsync(key);

                if (lastRequestTicks.HasValue)
                {
                    var lastRequest = new DateTimeOffset(long.Parse(lastRequestTicks!), TimeSpan.Zero);
                    var elapsed = DateTimeOffset.UtcNow - lastRequest;

                    if (elapsed.TotalMilliseconds < _minIntervalMs)
                    {
                        _logger.LogWarning(
                            "Rate limit exceeded for user {UserId}. Elapsed: {ElapsedMs}ms, Required: {RequiredMs}ms",
                            userId,
                            elapsed.TotalMilliseconds,
                            _minIntervalMs);
                        return true;
                    }
                }

                await db.StringSetAsync(key, nowTicks.ToString(), TimeSpan.FromMinutes(1));

                _logger.LogDebug("Rate limit check passed for user {UserId}", userId);
                return false;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking rate limit for user {UserId}", userId);
                return false;
            }
        }

        public async Task<bool> IsUserInQueueAsync(string userId, CancellationToken cancellationToken = default)
        {
            var db = _redis.GetDatabase();

            try
            {
                var waitingPlayers = await db.ListRangeAsync(WaitingPlayersKey);

                var isInQueue = waitingPlayers.Any(player => player.ToString() == userId);

                if (isInQueue)
                {
                    _logger.LogWarning("User {UserId} is already in the matchmaking queue", userId);
                }

                return isInQueue;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking if user {UserId} is in queue", userId);
                return false;
            }
        }
    }
}
