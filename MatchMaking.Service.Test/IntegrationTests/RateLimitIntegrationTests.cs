using FluentAssertions;
using MatchMaking.Service.Services.Concrete;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using StackExchange.Redis;

namespace MatchMaking.Service.Test.IntegrationTests;

public class RateLimitIntegrationTests
{
    [Fact]
    public async Task RateLimit_SequentialRequests_Within100ms_ShouldBeLimited()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<RedisRateLimitService>>();

        mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);
        mockConfigSection.Setup(x => x.Value).Returns("100");
        mockConfiguration.Setup(x => x.GetSection("RateLimit:MinIntervalMs"))
            .Returns(mockConfigSection.Object);

        var timestamps = new Queue<string>();

        mockDatabase.Setup(x => x.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => timestamps.Count > 0 ? new RedisValue(timestamps.Peek()) : RedisValue.Null);

        mockDatabase.Setup(x => x.StringSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<bool>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey k, RedisValue v, TimeSpan? t, bool b, When w, CommandFlags f) =>
            {
                timestamps.Enqueue(v.ToString());
                return true;
            });

        var service = new RedisRateLimitService(mockRedis.Object, mockConfiguration.Object, mockLogger.Object);
        var userId = "test-user";

        var result1 = await service.IsRateLimitedAsync(userId, CancellationToken.None);
        result1.Should().BeFalse("first request should not be rate limited");

        var result2 = await service.IsRateLimitedAsync(userId, CancellationToken.None);
        result2.Should().BeTrue("second request within 100ms should be rate limited");
    }

    [Fact]
    public async Task RateLimit_MultipleUsers_ShouldBeIndependent()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<RedisRateLimitService>>();

        mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);
        mockConfigSection.Setup(x => x.Value).Returns("100");
        mockConfiguration.Setup(x => x.GetSection("RateLimit:MinIntervalMs"))
            .Returns(mockConfigSection.Object);

        var userTimestamps = new Dictionary<string, string>();

        mockDatabase.Setup(x => x.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey key, CommandFlags flags) =>
            {
                var keyStr = key.ToString();
                return userTimestamps.ContainsKey(keyStr) ? new RedisValue(userTimestamps[keyStr]) : RedisValue.Null;
            });

        mockDatabase.Setup(x => x.StringSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<bool>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey k, RedisValue v, TimeSpan? t, bool b, When w, CommandFlags f) =>
            {
                userTimestamps[k.ToString()] = v.ToString();
                return true;
            });

        var service = new RedisRateLimitService(mockRedis.Object, mockConfiguration.Object, mockLogger.Object);

        var user1Result = await service.IsRateLimitedAsync("user1", CancellationToken.None);
        var user2Result = await service.IsRateLimitedAsync("user2", CancellationToken.None);

        user1Result.Should().BeFalse("user1 first request should not be limited");
        user2Result.Should().BeFalse("user2 should be independent of user1");
    }

    [Fact]
    public async Task RateLimit_ConcurrentRequests_SameUser_OnlyOneSucceeds()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<RedisRateLimitService>>();

        mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);
        mockConfigSection.Setup(x => x.Value).Returns("100");
        mockConfiguration.Setup(x => x.GetSection("RateLimit:MinIntervalMs"))
            .Returns(mockConfigSection.Object);

        var timestamps = new Queue<string>();
        var lockObj = new object();

        mockDatabase.Setup(x => x.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(() =>
            {
                lock (lockObj)
                {
                    return timestamps.Count > 0 ? new RedisValue(timestamps.Peek()) : RedisValue.Null;
                }
            });

        mockDatabase.Setup(x => x.StringSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<bool>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey k, RedisValue v, TimeSpan? t, bool b, When w, CommandFlags f) =>
            {
                lock (lockObj)
                {
                    timestamps.Enqueue(v.ToString());
                    return true;
                }
            });

        var service = new RedisRateLimitService(mockRedis.Object, mockConfiguration.Object, mockLogger.Object);
        var userId = "concurrent-user";

        var tasks = Enumerable.Range(0, 10)
            .Select(_ => service.IsRateLimitedAsync(userId, CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        results.Should().Contain(false, "at least one request should succeed");
        results.Should().Contain(true, "at least one request should be rate limited");
    }

    [Fact]
    public async Task RateLimit_RequestsWithDelay_ShouldAllSucceed()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<RedisRateLimitService>>();

        mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);
        mockConfigSection.Setup(x => x.Value).Returns("50");
        mockConfiguration.Setup(x => x.GetSection("RateLimit:MinIntervalMs"))
            .Returns(mockConfigSection.Object);

        string? lastTimestamp = null;

        mockDatabase.Setup(x => x.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => lastTimestamp != null ? new RedisValue(lastTimestamp) : RedisValue.Null);

        mockDatabase.Setup(x => x.StringSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<bool>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey k, RedisValue v, TimeSpan? t, bool b, When w, CommandFlags f) =>
            {
                lastTimestamp = v.ToString();
                return true;
            });

        var service = new RedisRateLimitService(mockRedis.Object, mockConfiguration.Object, mockLogger.Object);
        var userId = "delayed-user";

        var result1 = await service.IsRateLimitedAsync(userId, CancellationToken.None);
        result1.Should().BeFalse("first request should succeed");

        await Task.Delay(60);

        var result2 = await service.IsRateLimitedAsync(userId, CancellationToken.None);
        result2.Should().BeFalse("second request after delay should succeed");

        await Task.Delay(60);

        var result3 = await service.IsRateLimitedAsync(userId, CancellationToken.None);
        result3.Should().BeFalse("third request after delay should succeed");
    }
}
