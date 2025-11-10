using FluentResults;
using MatchMaking.Service.Services.Abstracts;
using StackExchange.Redis;

namespace MatchMaking.Service.Services.Concrete;

public class RedisRateLimitService(
    IConnectionMultiplexer redis,
    IConfiguration configuration,
    ILogger<RedisRateLimitService> logger,
    TimeProvider timeProvider) : IRateLimitService
{
    private readonly IConnectionMultiplexer _redis = redis;
    private readonly ILogger<RedisRateLimitService> _logger = logger;
    private readonly TimeProvider _timeProvider = timeProvider;
    private readonly int _minIntervalMs = configuration.GetValue<int>("RateLimit:MinIntervalMs", 100);
    private const string WaitingPlayersKey = "waiting_players";
    private const string RateLimitKeyPrefix = "ratelimit:{0}";

    public async Task<Result<bool>> IsRateLimitedAsync(string userId, CancellationToken cancellationToken)
    {
        var db = _redis.GetDatabase();
        var key = string.Format(RateLimitKeyPrefix, userId);

        try
        {
            var nowTicks = _timeProvider.GetUtcNow().Ticks;
            var lastRequestTicks = await db.StringGetAsync(key);

            if (lastRequestTicks.HasValue &&
                long.TryParse(lastRequestTicks!, out var ticks))
            {
                var lastRequest = new DateTimeOffset(ticks, TimeSpan.Zero);
                var elapsed = _timeProvider.GetUtcNow() - lastRequest;

                if (elapsed.TotalMilliseconds < _minIntervalMs)
                {
                    _logger.LogWarning(
                        "Rate limit exceeded for user {UserId}. Elapsed: {ElapsedMs}ms, Required: {RequiredMs}ms",
                        userId,
                        elapsed.TotalMilliseconds,
                        _minIntervalMs);

                    return Result.Ok(true);
                }
            }

            await db.StringSetAsync(key, nowTicks.ToString(), TimeSpan.FromMinutes(1));
            _logger.LogDebug("Rate limit check passed for user {UserId}", userId);
            return Result.Ok(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking rate limit for user {UserId}", userId);
            return Result.Fail<bool>(new Error("Error checking rate limit").CausedBy(ex));
        }
    }

    public async Task<Result<bool>> IsUserInQueueAsync(string userId, CancellationToken cancellationToken)
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

            return Result.Ok(isInQueue);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking if user {UserId} is in queue", userId);
            return Result.Fail<bool>(new Error("Error checking queue presence").CausedBy(ex));
        }
    }
}
