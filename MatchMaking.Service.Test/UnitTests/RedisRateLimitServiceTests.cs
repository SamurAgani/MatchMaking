using FluentAssertions;
using MatchMaking.Service.Services.Concrete;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using StackExchange.Redis;

namespace MatchMaking.Service.Test.UnitTests;

public class RedisRateLimitServiceTests
{
    private readonly Mock<IConnectionMultiplexer> _mockRedis;
    private readonly Mock<IDatabase> _mockDatabase;
    private readonly Mock<IConfiguration> _mockConfiguration;
    private readonly Mock<IConfigurationSection> _mockConfigSection;
    private readonly Mock<ILogger<RedisRateLimitService>> _mockLogger;
    private readonly RedisRateLimitService _service;

    public RedisRateLimitServiceTests()
    {
        _mockRedis = new Mock<IConnectionMultiplexer>();
        _mockDatabase = new Mock<IDatabase>();
        _mockConfiguration = new Mock<IConfiguration>();
        _mockConfigSection = new Mock<IConfigurationSection>();
        _mockLogger = new Mock<ILogger<RedisRateLimitService>>();

        _mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(_mockDatabase.Object);

        _mockConfigSection.Setup(x => x.Value).Returns("100");
        _mockConfiguration.Setup(x => x.GetSection("RateLimit:MinIntervalMs"))
            .Returns(_mockConfigSection.Object);

        _service = new RedisRateLimitService(
            _mockRedis.Object,
            _mockConfiguration.Object,
            _mockLogger.Object);
    }

    [Fact]
    public async Task IsRateLimitedAsync_FirstRequest_ReturnsFalse()
    {
        var userId = "user123";
        _mockDatabase.Setup(x => x.StringGetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);

        _mockDatabase.Setup(x => x.StringSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<bool>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);

        var result = await _service.IsRateLimitedAsync(userId, CancellationToken.None);

        result.Value.Should().BeFalse();
    }

    [Fact]
    public async Task IsRateLimitedAsync_RequestWithin100ms_ReturnsTrue()
    {
        var userId = "user123";
        var recentTimestamp = DateTimeOffset.UtcNow.AddMilliseconds(-50).Ticks.ToString();

        _mockDatabase.Setup(x => x.StringGetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(new RedisValue(recentTimestamp));

        var result = await _service.IsRateLimitedAsync(userId, CancellationToken.None);

        result.Value.Should().BeTrue();
    }

    [Fact]
    public async Task IsRateLimitedAsync_RequestAfter100ms_ReturnsFalse()
    {
        var userId = "user123";
        var oldTimestamp = DateTimeOffset.UtcNow.AddMilliseconds(-150).Ticks.ToString();

        _mockDatabase.Setup(x => x.StringGetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(new RedisValue(oldTimestamp));

        _mockDatabase.Setup(x => x.StringSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<bool>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);

        var result = await _service.IsRateLimitedAsync(userId, CancellationToken.None);

        result.Value.Should().BeFalse();
    }

    [Fact]
    public async Task IsUserInQueueAsync_WhenUserInQueue_ReturnsTrue()
    {
        var userId = "user123";
        var queueData = new RedisValue[] { "user1", "user123", "user3" };

        _mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(queueData);

        var result = await _service.IsUserInQueueAsync(userId, CancellationToken.None);

        result.Value.Should().BeTrue();
    }

    [Fact]
    public async Task IsUserInQueueAsync_WhenUserNotInQueue_ReturnsFalse()
    {
        var userId = "user123";
        var queueData = new RedisValue[] { "user1", "user2", "user3" };

        _mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(queueData);

        var result = await _service.IsUserInQueueAsync(userId, CancellationToken.None);

        result.Value.Should().BeFalse();
    }
}
