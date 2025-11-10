using FluentResults;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;
using StackExchange.Redis;
using System.Text.Json;

namespace MatchMaking.Service.Services.Concrete;

public class RedisMatchStorageService(
    IConnectionMultiplexer redis,
    ILogger<RedisMatchStorageService> logger) : IMatchStorageService
{
    private readonly IConnectionMultiplexer _redis = redis;
    private readonly ILogger<RedisMatchStorageService> _logger = logger;
    private const string UserMatchKeyPrefix = "user:{0}:match";

    public async Task<Result> StoreMatchForUserAsync(string userId, MatchComplete matchComplete, CancellationToken cancellationToken)
    {
        var db = _redis.GetDatabase();
        var key = string.Format(UserMatchKeyPrefix, userId);

        try
        {
            var matchJson = JsonSerializer.Serialize(matchComplete);
            await db.StringSetAsync(key, matchJson, TimeSpan.FromHours(1));
            return Result.Ok();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to store match for user {UserId}", userId);
            return Result.Fail(new Error("Failed to store match for user").CausedBy(ex)
                .WithMetadata(MetadataKeys.UserId, userId));
        }
    }

    public async Task<Result<MatchComplete?>> GetMatchForUserAsync(string userId, CancellationToken cancellationToken)
    {
        var db = _redis.GetDatabase();
        var key = string.Format(UserMatchKeyPrefix, userId);

        try
        {
            var matchJson = await db.StringGetAsync(key);

            if (!matchJson.HasValue)
            {
                _logger.LogDebug("No match found for user {UserId}", userId);
                return Result.Ok<MatchComplete?>(null);
            }

            MatchComplete? matchComplete;

            matchComplete = JsonSerializer.Deserialize<MatchComplete>(matchJson!);

            return Result.Ok<MatchComplete?>(matchComplete);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to retrieve match for user {UserId}", userId);
            return Result.Fail<MatchComplete?>(new Error("Failed to retrieve match for user").CausedBy(ex)
                .WithMetadata(MetadataKeys.UserId, userId));
        }
    }
}
