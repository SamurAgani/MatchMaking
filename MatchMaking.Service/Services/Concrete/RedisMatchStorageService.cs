using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Models;
using StackExchange.Redis;
using System.Text.Json;

namespace MatchMaking.Service.Services.Concrete
{
    public class RedisMatchStorageService : IMatchStorageService
    {
        private readonly IConnectionMultiplexer _redis;
        private readonly ILogger<RedisMatchStorageService> _logger;

        public RedisMatchStorageService(
            IConnectionMultiplexer redis,
            ILogger<RedisMatchStorageService> logger)
        {
            _redis = redis;
            _logger = logger;
        }

        public async Task StoreMatchForUserAsync(string userId, MatchComplete matchComplete, CancellationToken cancellationToken = default)
        {
            var db = _redis.GetDatabase();
            var key = $"user:{userId}:match";

            try
            {
                var matchJson = JsonSerializer.Serialize(matchComplete);

                await db.StringSetAsync(key, matchJson, TimeSpan.FromHours(1));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to store match for user {UserId}", userId);
                throw;
            }
        }

        public async Task<MatchComplete?> GetMatchForUserAsync(string userId, CancellationToken cancellationToken = default)
        {
            var db = _redis.GetDatabase();
            var key = $"user:{userId}:match";

            try
            {
                var matchJson = await db.StringGetAsync(key);

                if (!matchJson.HasValue)
                {
                    _logger.LogDebug("No match found for user {UserId}", userId);
                    return null;
                }

                var matchComplete = JsonSerializer.Deserialize<MatchComplete>(matchJson!);

                return matchComplete;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to retrieve match for user {UserId}", userId);
                throw;
            }
        }
    }
}
